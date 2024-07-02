import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Load datasets
# taken directly for csv for better readability and better exploration of the data
temperature_df = pd.read_csv('./data/climate-change-earth-surface-temperature-data/GlobalLandTemperaturesByCountry.csv')
displacement_df = pd.read_csv('./internally-displaced-persons-from-disasters.csv')

# Countries in South Asian Sector
south_asian_countries = [
    'Afghanistan', 'Bangladesh', 'Bhutan', 'India', 'Maldives', 'Nepal', 'Pakistan', 'Sri Lanka'
]

# Extract the temperature and putting it in year format
temperature_df['dt'] = pd.to_datetime(temperature_df['dt'])
temperature_df['Year'] = temperature_df['dt'].dt.year

# Filteration of the data for the years 2008-2013
temperature_df = temperature_df[(temperature_df['Year'] >= 2008) & (temperature_df['Year'] <= 2013)]
displacement_df = displacement_df[(displacement_df['Year'] >= 2008) & (displacement_df['Year'] <= 2013)]

# Renaming 'Entity' to 'Country' in displacement dataset for consistency
displacement_df.rename(columns={'Entity': 'Country'}, inplace=True)

# Filteration for South Asian countries
temperature_df = temperature_df[temperature_df['Country'].isin(south_asian_countries)]
displacement_df = displacement_df[displacement_df['Country'].isin(south_asian_countries)]

# Grouping temperature data by country and year, and calculate averages
temperature_summary = temperature_df.groupby(['Country', 'Year']).agg({
    'AverageTemperature': 'mean',
    'AverageTemperatureUncertainty': 'mean'
}).reset_index()

# Aggregatating total displacement by country and year
displacement_summary = displacement_df.groupby(['Country', 'Year']).agg({
    'Internally displaced persons, new displacement associated with disasters (number of cases)': 'sum'
}).reset_index()

# Merging the summarized datasets on 'Country' and 'Year'
merged_summary = pd.merge(temperature_summary, displacement_summary, on=['Country', 'Year'])

# Rounding up the temperature data to two decimal places
merged_summary['AverageTemperature'] = merged_summary['AverageTemperature'].round(2)
merged_summary['AverageTemperatureUncertainty'] = merged_summary['AverageTemperatureUncertainty'].round(2)

# Renaming the displacement column for simplicity
merged_summary.rename(columns={
    'Internally displaced persons, new displacement associated with disasters (number of cases)': 'Displacement'
}, inplace=True)

# Plotting trend analysis for all South Asian countries in one plot
def plot_trend_analysis_all_south_asia():
    fig, ax1 = plt.subplots(figsize=(16, 10))
    
    colors = plt.get_cmap('tab20', len(south_asian_countries))
    displacement_lines = []
    temperature_lines = []

    for i, country in enumerate(south_asian_countries):
        country_data = merged_summary[merged_summary['Country'] == country]
        if not country_data.empty:
            displacement_line, = ax1.plot(country_data['Year'], country_data['Displacement'], marker='o', linestyle='-', label=f'{country} Displacement', color=colors(i))
            displacement_lines.append(displacement_line)

    ax1.set_xlabel('Year')
    ax1.set_ylabel('Displaced Persons')

    # Creating a second y-axis to plot average temperature
    ax2 = ax1.twinx()
    
    for i, country in enumerate(south_asian_countries):
        country_data = merged_summary[merged_summary['Country'] == country]
        if not country_data.empty:
            temperature_line, = ax2.plot(country_data['Year'], country_data['AverageTemperature'], marker='x', linestyle='--', label=f'{country} Temperature', color=colors(i))
            temperature_lines.append(temperature_line)

    ax2.set_ylabel('Average Temperature (°C)')
    
    # Adding legends
    displacement_legend = ax1.legend(handles=displacement_lines, loc='upper left', bbox_to_anchor=(1.05, 1), title='Displacement')
    temperature_legend = ax2.legend(handles=temperature_lines, loc='lower left', bbox_to_anchor=(1.05, 0.6), title='Temperature')

    ax1.add_artist(displacement_legend)
    
    # Adding title and show the plot
    plt.title('Trend Analysis for South Asian Countries (2008-2013)')
    fig.tight_layout()  
    plt.show()

# Ploting trend analysis for all South Asian countries
plot_trend_analysis_all_south_asia()

# Saving merged dataframe to csv for verification
merged_summary.to_csv('./data/merged_summary_trend_analysis_south_asia.csv', index=False)

# Displaying the first few rows of the merged dataframe
print(merged_summary.head())

# Calculating the correlation for each country
correlation_dict = {}

for country in south_asian_countries:
    country_data = merged_summary[merged_summary['Country'] == country][['AverageTemperature', 'Displacement']]
    if len(country_data) > 1:
        correlation_matrix = country_data.corr()
        correlation_value = correlation_matrix.loc['AverageTemperature', 'Displacement']
        correlation_dict[country] = correlation_value

# Creating a DataFrame from the correlation dictionary
correlation_df = pd.DataFrame.from_dict(correlation_dict, orient='index', columns=['Correlation'])

# Plotting correlation heatmap
def plot_correlation_heatmap(correlation_df):
    plt.figure(figsize=(10, 6))
    sns.heatmap(correlation_df, annot=True, cmap='coolwarm', center=0, vmin=-1, vmax=1)
    plt.title('Correlation between Average Temperature and Displacement (2008-2013)')
    plt.show()

# Plotting correlation heatmap
plot_correlation_heatmap(correlation_df)

# Saving correlation dataframe to csv for verification
correlation_df.to_csv('./data/correlation_matrix_south_asia.csv', index=True)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import os

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode
chrome_options.add_argument("--disable-gpu")  # Disable GPU rendering

# Set up the WebDriver using ChromeDriverManager
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Open the webpage
driver.get("https://ourworldindata.org/grapher/internally-displaced-persons-from-disasters")

# Wait for the page to load
time.sleep(5)

# Close the cookie notice if it appears
try:
    cookie_close_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Reject cookies']")
    cookie_close_button.click()
except:
    pass

# Wait for the cookie notice to close
time.sleep(10)

# Find and click the download button
download_button = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Download']")
download_button.click()

# Wait for the download options to appear
time.sleep(2)

# Click the CSV download option
csv_button = driver.find_element(By.XPATH, "//h4[contains(text(),'Full data (CSV)')]")
csv_button.click()

# Wait for the download to complete
time.sleep(10)

# Clean up and close the browser
driver.quit()

download_dir = os.path.expanduser('./')
file_path = os.path.join(download_dir, 'internally-displaced-persons-from-disasters.csv')
df = pd.read_csv(file_path)

# Display the first few rows of the dataset
print(df.head())

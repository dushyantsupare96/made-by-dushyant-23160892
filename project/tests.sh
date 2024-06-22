#!/bin/bash

# Run the pipeline in test mode
python3 -m venv venv
source ./venv/bin/activate
pip3 install -r requirements.txt
#python3 kagglejsoncreation.py
pytest ./pipeline-test.py

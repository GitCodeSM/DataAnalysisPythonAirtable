# DataAnalysisPythonAirtable

-- Create the main folder structure
- to contain all the extracted zipped all market data files.
- to contain the wms excel file.
- to contain the main python codes files.
- to contain output files.

-- Create the sku_msku_mapper.py which contains
- SKUMapper class with features like
  1. class variable to collect dataframes for sku_msku mapped data and combo data of multiple mskus.
  2. class variable to collect logs of process.
  3. master mapper method to
    - process all market types data into one useful dataframe that will be used to create final mapped data.
    - load sku_msku_sheet data and combo sheet data from wms excel file.
    - call wms sheet data processing methods.
  4. sku_msku loader method to process all sku_msku sheet data for mapping the skus with mskus, status, date, panel, warehouse.
  5. combo loader method to process all combo sheet data for mapping the skus with mskus, status, date, panel, warehouse.
  6. get_mapping method to get the collected final dataframe to store in csv or desired database.
  7. get_logs method to return all the process logs.
  8. handling missing values, error handling, date format handling, summing quantity for each sku.

-- Create Airtable account
  - add a base
  - create table by importing your final output saved in csv format or load from a database.
  - use airtable features to analyse and visualize data.

-- Usage tips:
  - clone this github repository with: git clone https://github.com/GitCodeSM/DataAnalysisPythonAirtable.git
  - use cd DataAnalysisPythonAirtable/interview_task
  - download the required libraries from requirements.txt
  - make sure you have python 3.10 and above installed.
  - make sure you have csv and excel extension installed to read such files.
  - use python3 sku_msku_mapper.py to run the file.

## TODO 
- Need to add table to Airtable with Airtable API and python.
- Need to use Airtable AI for analysis and visualization.
- Need to add simple user friendly front-end to
   1. load and save the wms excel file.
   2. load, extract and save zipped market data csv files.

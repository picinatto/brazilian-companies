import sqlite3
import pandas as pd
import numpy as np
import sys

new_db_location = '/media/sf_share/data/out/'
new_db_name = 'companies'

def run(export_format='csv'):
  available_formats = ['csv','sqlite']
  if export_format not in available_formats:
    print('You have chosen an invalid format')
  else:
    print('Starting the exporting process...')
    export_data(export_format)

def export_data(export_foram):
  # Connect to the database to get the data that is going to be exported
  conDB_new = sqlite3.connect(new_db_location+new_db_name)
  exported_path = 'exports/csv/companies.csv'
  
  # Getting the data from the table companies
  print('Getting the data from the database..')
  df_companies = pd.read_sql('SELECT * FROM companies;', conDB_new)

  print('Exporting to csv')
  df_companies.to_csv(exported_path)

  print(f'Process finished. The file was saved in: {exported_path}')

# Run the application
inputed_format = sys.argv[1]
run(inputed_format)
import sqlite3
import pandas as pd
import numpy as np
import sys

new_db_location = '/media/sf_share/data/out/'
new_db_name = 'companies'

def run():

  # Ask the user for the expected export format
  export_format = ''
  while export_format not in ['csv','sqlite']:
    export_format = input('Which format would you like to export the data (csv / sqlite)?: ').lower()

  # Ask the user if wants only active companies
  active_companies = ''
  while active_companies not in ['y', 'n']:
    active_companies = input('Would like only active companies (y / n)?: ').lower()

  # TODO: Filter states
  # TODO: Filter only headquarters

  print(export_format)
  print(active_companies)

  print('Starting the exporting process...')
  export_data(export_format, active_companies)

def export_data(export_format, active_companies):
  # Connect to the database to get the data that is going to be exported
  conDB_new = sqlite3.connect(new_db_location+new_db_name)
  
  # Getting the data from the table companies
  print('Getting the data from the database..')
  sql_select = 'SELECT * FROM companies'

  # Using logic to filter data as asked
  if active_companies == 'y':
    sql_select += ' WHERE situacao_cadastral = "02"'
  else:
    sql_select += ';'

  df_companies = pd.read_sql(sql_select, conDB_new)

  if export_format == 'csv':
    print('Exporting to csv')
    exported_path = 'exports/companies.csv'
    df_companies.to_csv(exported_path)
  elif export_format == 'sqlite': 
    print('Exporting to sql')
    exported_path = 'exports/companies.db'
    # TODO: Add the logic to save to sqlite
    df_companies.to_sql()
  else:
    print('No correct export format was found..')

  print(f'Process finished. The file was saved in: {exported_path}')

# Run the application
run()
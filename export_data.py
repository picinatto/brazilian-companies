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

  # Ask the user if wants only headquarters (matriz)
  headquarter = ''
  while headquarter not in ['y', 'n']:
    headquarter = input('Would like only the companies headquarters (y / n)?: ').lower()

  # Ask the user if wants only headquarters (matriz)
  remove_mei = ''
  while remove_mei not in ['y', 'n']:
    remove_mei = input('Would you like to remove MEI (Micro Entrepreneurs) (y / n)?: ').lower()

  # TODO: Filter states


  print('Starting the exporting process...')
  export_data(export_format, active_companies, headquarter, remove_mei)

def export_data(export_format, active_companies, headquarter, remove_mei):
  # Connect to the database to get the data that is going to be exported
  conDB_new = sqlite3.connect(new_db_location+new_db_name)
  
  # Getting the data from the table companies
  print('Getting the data from the database..')
  sql_select_companies = 'SELECT * FROM companies'

  # Using logic to filter data as asked
  if active_companies == 'y':
    sql_select_companies += ' WHERE situacao = "02"'
    if headquarter == 'y':
      sql_select_companies += ' AND matriz_filial = "1"'
      if remove_mei == 'y':
        sql_select_companies += ' AND porte IN("00","03","05")'
  else:
    if headquarter == 'y':
      sql_select_companies += ' WHERE matriz_filial ="1"'
      if remove_mei == 'y':
        sql_select_companies += ' AND porte IN("00","03","05")'
    else:
      if remove_mei == 'y':
        sql_select_companies += ' WHERE porte IN("00","03","05")'

  
  # Adding ; at the end of sql statement
  sql_select_companies += ';'

  df_companies = pd.read_sql(sql_select_companies, conDB_new)

  # Initialize the variable that will hold a string with all the cnpjs
  cnpj_list = ''
  # Iterate each item in the list and convert to a string
  for key, value in df_companies.iterrows():
    cnpj_list += '"' + value[0] + '",'
  # Remove the last comma from the string
  cnpj_list = cnpj_list[:-1]

  df_cnaes = pd.read_sql(f'SELECT * FROM cnaes WHERE cnpj IN({cnpj_list})', conDB_new)

  df_cities = pd.read_sql('SELECT * FROM cities', conDB_new)

  df_cnaes_ibge = pd.read_sql('SELECT * FROM cnaes_ibge', conDB_new)

  if export_format == 'csv':
    print('Exporting to csv')
    exported_path = 'exports/'
    df_companies.to_csv(exported_path+'companies.csv')
    df_cnaes.to_csv(exported_path+'cnaes.csv')
    df_cities.to_csv(exported_path+'cities.csv')
    df_cnaes_ibge.to_csv(exported_path+'cnaes_ibge.csv')
  elif export_format == 'sqlite': 
    print('Exporting to sql')
    exported_path = 'exports/companies.db'
    # Create the new database if does not exists
    conDB_export = sqlite3.connect(exported_path)
    cursorDB_export = conDB_new.cursor()
    df_companies.to_sql()
  else:
    print('No correct export format was found..')

  print(f'Process finished. The file was saved in: {exported_path}')

# Run the application
run()
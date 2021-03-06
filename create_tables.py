import sqlite3
import datetime
import pandas as pd
import numpy as np
from unidecode import unidecode
import sql_scripts as sql

db_location = '/media/sf_share/data/out/'
db_name = 'CNPJ_full.db'
db_short_name = 'CNPJ_full'
new_db_location = db_location
new_db_name = 'companies'

def create_table_companies_filtered_state(states_list,status=''):
  ''' 
    Creates a table and add data on SQLITE with filtered data for the states listed as params
      to reduce the size of the dataset and time for querying
    Receives a list of states as parameter and by default only active companies are saved. If 
      needs all companies, pass any string as second argument
  '''
  table_name = 'companies'
  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')

  # Create the new database if does not exists
  conDB_new = sqlite3.connect(new_db_location+new_db_name)
  cursorDB_new = conDB_new.cursor()

  # Attach new database to the old one
  conDB_new.execute(f"ATTACH '{db_location}{db_name}' AS '{db_short_name}';")

  print(f'Creating the table companies if not exists in database {new_db_name}..')
  
  cursorDB_new.execute(sql.get_sql_create_companies())

  # Delete existing data in the table
  print(f'Cleaning the table {table_name}') 
  cursorDB_new.execute(sql.get_sql_delete(table_name))

  # Initialize the variable that will hold a string with all the states
  states = ''
  # Iterate each item in the list and convert to a string
  for state in states_list:
    states += '"' + state + '",'
  # Remove the last comma from the string
  states = states[:-1]

  # Inserting the data in the table
  print(f'Creating the insert statement in the table {table_name} on DB {new_db_name}')
  # Getting the sql string from class
  sql_insert = sql.get_sql_insert_companies(db_short_name, states)
  
  # Checking if a string was added to status, if not add the filter condition
  #   to get only the Active companies (situacao == '02')
  if not status:
    sql_insert = sql_insert[:-2]
    sql_insert += ' AND situacao = "02");'

  # Execute the inserting
  print(f'Inserting data in the table {table_name} DB {new_db_name} for the state(s) {states}')
  cursorDB_new.execute(sql_insert)
  conDB_new.commit()

  print(f'Creating indexes for the {table_name} table')
  indexes = {
    f'{table_name}_cod_municipio': {'table':f'{table_name}','column':'cod_municipio'},
    f'{table_name}_cnpj': {'table':f'{table_name}','column':'cnpj'},
    f'{table_name}_porte': {'table':f'{table_name}','column':'porte'}
  }

  for index in indexes:
    sql_drop_index = f'DROP INDEX IF EXISTS {index}'
    cursorDB_new.execute(sql_drop_index)

  for key in indexes:
    sql_insert_index = f'CREATE INDEX {key} ON {indexes[key]["table"]} ({indexes[key]["column"]})'
    cursorDB_new.execute(sql_insert_index)

  print('Getting data for the cnpjs filtered and the main cnae')
  # Creating dataframe to get each CNPJ to process the cnaes
  df_cnpjs = pd.read_sql(f'SELECT cnpj, cnae_fiscal FROM {table_name}', conDB_new)
  # Get the cnaes for the filtered companies
  print('Start the process of getting all the cnaes for each company')
  df_cnaes = get_cnaes(df_cnpjs)

  print(f'Creating the table cnaes if not exists in database {new_db_name}..')
  cursorDB_new.execute(sql.get_sql_create_cnaes())

  # Delete existing data in the table
  print(f'Cleaning the table cnaes')
  cursorDB_new.execute(sql.get_sql_delete('cnaes'))

  print(f'Saving the cnaes data in the database {new_db_name}')
  df_cnaes.to_sql('cnaes', con=conDB_new, if_exists='replace',index=False)

  print(f'Creating indexes for the cnaes table')
  indexes = {
    'cnaes_cnpj': {'table':'cnaes','column':'cnpj'},
  }

  for index in indexes:
    sql_drop_index = f'DROP INDEX IF EXISTS {index}'
    cursorDB_new.execute(sql_drop_index)

  for key in indexes:
    sql_insert_index = f'CREATE INDEX {key} ON {indexes[key]["table"]} ({indexes[key]["column"]})'
    cursorDB_new.execute(sql_insert_index)

  #TODO: Get the stockholders data

  # Get CNAES from IBGE and add to the Database
  df_cnaes_ibge = pd.read_csv('assets/cnaes_ibge.csv',dtype='str')

  cursorDB_new.execute(sql.get_sql_create_cnaes_ibge())

  # Delete existing data in the table
  print(f'Cleaning the table cnaes_ibge') 
  cursorDB_new.execute(sql.get_sql_delete('cnaes_ibge'))

  # Write the table to the Database
  df_cnaes_ibge.to_sql('cnaes_ibge', con=conDB_new, if_exists='replace')

  print(f'Finished at {datetime.datetime.now()}')
  conDB_new.close()

def get_cnaes(cnpjs):
  '''
    Receives the filtered cnpjs as a df
    Join then with cnaes_secundario to remove the one that are not filtered/selected
    Returns a Pandas Data Frame with CNPJ and CNAES
  '''
  print('Getting the unique CNPJs filtered')
  # Initialize the variable that will hold a string with all the cnpjs
  cnpj_list = ''
  # Iterate each item in the list and convert to a string
  for key, value in cnpjs.iterrows():
    cnpj_list += '"' + value[0] + '",'
  # Remove the last comma from the string
  cnpj_list = cnpj_list[:-1]

  print(f'Getting the data from the secondary cnaes in the DB {db_name}')
  conDB = sqlite3.connect(db_location+db_name)
  df_all_cnaes = pd.read_sql(f'SELECT * FROM cnaes_secundarios WHERE cnpj IN({cnpj_list});', conDB)

  # Adjusting the CNPJ table to be identical to cnaes table
  cnpjs.rename(columns={'cnae_fiscal':'cnae'}, inplace=True)
  cnpjs['cnae_ordem'] = 0
  # Reordering the columns
  cnpjs = cnpjs[['cnpj','cnae_ordem','cnae']]

  # Adding 1 to the cnae ordem to fix the cnae_fiscal that was on cnpjs table
  df_all_cnaes['cnae_ordem'] = [cnae_ordem + 1 for cnae_ordem in df_all_cnaes['cnae_ordem']]

  print('Appending the data from both dataframes')  
  #Appending the two dataframes...
  df_cnaes = pd.concat([df_all_cnaes,cnpjs],ignore_index=True)
  #df_all_cnaes.append(cnpjs, ignore_index=True, verify_integrity=False)

  return df_cnaes

def create_table_cities():
  '''
      Get the cities from the empresas table and match each by the city name and 
      state code with the IBGE city data. IBGE city data has more levels and is 
      the Brazilian standard to link cities with systems
      The function create a table 'cities' with the column 'cod_municipio' that can
      be linked the empresas on 'cod_municipio' and additional data like meso and 
      microregion and the city_code for the IBGE system
  '''
  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')

  conDB = sqlite3.connect(db_location+db_name)
  cursorDB = conDB.cursor()

  # Create the new database if does not exists
  conDB_new = sqlite3.connect(new_db_location+new_db_name)
  cursorDB_new = conDB_new.cursor()

  # Select the unique cities from the companies table and store in df_cities_companies
  print('Getting the cities from the companies\' database')
  df_cities_companies = pd.read_sql_query(sql.get_sql_select_cities(),conDB)
  # Write the cities_companies to csv for auditing purpuses
  df_cities_companies.to_csv('assets/cities-companies.csv')

  # Get the cities from the IBGE CSV downloaded from the following link:
  # https://ibge.gov.br/explica/codigos-dos-municipio.php
  # PS: some cities have wrong names, so I fixed the csv file, to
  #   get same results use the file 'cities_brazi.csv' in assets folder
  print('Getting the cities from the IBGE csv file')
  df_cities_ibge = pd.read_csv('assets/cities_brazil.csv',dtype='str')

  # Remove Brazilian-Portuguese accents and special characteres for the city_name to 
  #  match the IBGE unicode format
  df_cities_ibge['city_name'] = [unidecode(x.upper()) for x in df_cities_ibge['city_name']]

  # Merge the tables by the uf and city name
  print('Merging the companies cities and IBGE')
  df_cities = pd.merge(df_cities_ibge,df_cities_companies,how='left',on=['uf_name','city_name'])

  print('Creating table cities, if not existent')
  cursorDB_new.execute(sql.get_sql_create_cities())

  # Clean the table cities to not duplicate records
  print('Cleaning the table cities')
  cursorDB_new.execute(sql.get_sql_delete('cities'))

  # Insert the cities to the city table
  print('Adding new cities to the table')
  df_cities.to_sql('cities', con=conDB_new, if_exists='replace')

  # Check if there is cities that were not matched by the uf and city_name
  print('Checking if there was not matched cities')
  # Join all the cities found by companies with cities found by join city_name|state
  df_cities_not_found = pd.merge(df_cities_companies,df_cities,how='outer',on='cod_municipio')
  # Filter only the cities found by companies and that were not found by the join
  # Filter only the 'cod_municipio','city_name_x','uf_name_x' columns
  df_cities_not_found = df_cities_not_found[(df_cities_not_found.cod_municipio.notnull()) & (df_cities_not_found.city_code.isnull())][['cod_municipio','city_name_x','uf_name_x']]

  # Check if there is city(ies) with that were not found in the join
  if not df_cities_not_found.empty:
    # Save the result to csv
    df_cities_not_found.to_csv('assets/cities-not-found.csv')
    # Print the result to the user
    print('Some cities that did not match city_name and uf with the IBGE data.')
    print(df_cities_not_found)
  else:
    print('There was no not matched cities')

  # Finish the process
  print(f'Finished at {datetime.datetime.now()}')
  conDB.close()

create_table_companies_filtered_state(['PR','SC','RS'])
create_table_cities()
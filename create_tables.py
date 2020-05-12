import sqlite3
import datetime
import pandas as pd
import numpy as np
from unidecode import unidecode

db_location = '/media/sf_share/data/out/'
db_name = 'CNPJ_full.db'

def create_table_companies_filtered_state(states_list,status=''):
  ''' 
    Creates a table and add data on SQLITE with filtered data for the states listed as params
      to reduce the size of the dataset and time for querying
    Receives a list of states as parameter and by default only active companies are saved. If 
      needs all companies, pass any string as second argument
  '''

  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')
  
  conDB = sqlite3.connect(db_location+db_name)
  cursorDB = conDB.cursor()

  print(f'Creating the table companies_filtered if not exists..')
  sql_create = '''CREATE TABLE IF NOT EXISTS companies_filtered (
      cnpj text, 
      matriz_filial text, 
      razao_social text, 
      nome_fantasia text, 
      situacao text, 
      data_situacao text, 
      motivo_situacao text, 
      nm_cidade_exterior text,
      cod_pais text, 
      nome_pais text, 
      cod_nat_juridica text, 
      data_inicio_ativ text, 
      cnae_fiscal text, 
      tipo_logradouro text, 
      logradouro text, 
      numero text, 
      complemento text, 
      bairro text, 
      cep text, 
      uf text, 
      cod_municipio text, 
      municipio text, 
      ddd_1 text, 
      telefone_1 text, 
      ddd_2 text, 
      telefone_2 text, 
      ddd_fax text, 
      num_fax text, 
      email text, 
      qualif_resp text, 
      capital_social integer, 
      porte text, 
      opc_simples text, 
      data_opc_simples text, 
      data_exc_simples text, 
      opc_mei text, 
      sit_especial text, 
      data_sit_especial
      );
   '''

  cursorDB.execute(sql_create)

  # Delete existing data in the table
  print('Cleaning the table companies_filtered')
  sql_delete = 'DELETE FROM companies_filtered'
  cursorDB.execute(sql_delete)


  # Initialize the variable that will hold a string with all the states
  states = ''
  # Iterate each item in the list and convert to a string
  for state in states_list:
    states += '"' + state + '",'
  # Remove the last comma from the string
  states = states[:-1]


  # Inserting the data in the table
  print('Creating the insert statement')

  sql_insert = f'''INSERT INTO companies_filtered SELECT * FROM (SELECT
      cnpj, 
      matriz_filial, 
      razao_social, 
      nome_fantasia, 
      situacao, 
      data_situacao, 
      motivo_situacao, 
      nm_cidade_exterior,
      cod_pais, 
      nome_pais, 
      cod_nat_juridica, 
      data_inicio_ativ, 
      cnae_fiscal, 
      tipo_logradouro, 
      logradouro, 
      numero, 
      complemento, 
      bairro, 
      cep, 
      uf, 
      cod_municipio, 
      municipio, 
      ddd_1, 
      telefone_1, 
      ddd_2, 
      telefone_2, 
      ddd_fax, 
      num_fax, 
      email, 
      qualif_resp, 
      CAST((capital_social/100) AS INTEGER), 
      porte, 
      opc_simples, 
      data_opc_simples, 
      data_exc_simples, 
      opc_mei, 
      sit_especial, 
      data_sit_especial
      FROM empresas WHERE uf IN({states}));'''
  
  # Checking if a string was added to status, if not add the filter condition
  #   to get only the Active companies (situacao == '02')
  if not status:
    sql_insert = sql_insert[:-2]
    sql_insert += ' AND situacao = "02");'

  # Execute the inserting
  print(f'Inserting data in the table companies_filtered for the state(s) {states}')
  cursorDB.execute(sql_insert)

  # Creating indexes
  print('Creating indexes for the companies_filtered..')
  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_cod_municipio', 'companies_filtered', 'cod_municipio')
  cursorDB.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_nome_municipio', 'companies_filtered', 'municipio')
  cursorDB.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_cnpj', 'companies_filtered', 'cnpj')
  cursorDB.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_porte', 'companies_filtered', 'porte')
  cursorDB.execute(sql_index)

  print(f'Finished at {datetime.datetime.now()}')
  conDB.close()

def create_table_cities():
  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')

  conDB = sqlite3.connect(db_location+db_name)
  cursorDB = conDB.cursor()

  sql_select_cities = ''' SELECT DISTINCT 
      t0.cod_municipio,
      t1.municipio AS city_name,
      t1.uf AS uf_name
    FROM empresas t0
      INNER JOIN (
        SELECT 
          cod_municipio,
          MIN(municipio) As municipio,
          MIN(uf) AS uf
        FROM empresas
          GROUP BY cod_municipio 
        --LIMIT 50
      ) t1 ON t1.cod_municipio = t0.cod_municipio
    --LIMIT 50
  '''

  print('Getting the cities from the companies\' database')
  df_cities_companies = pd.read_sql_query(sql_select_cities,conDB)
  print(df_cities_companies)
  df_cities_companies.to_csv('cities-companies.csv')

  # Get the cities from the IBGE CSV downloaded
  print('Getting the cities from the IBGE csv')
  df_cities_ibge = pd.read_csv('assets/cities_brazil.csv',dtype='str')


  # Remove special characteres
  #df_cities_ibge['city_name'] = unidecode(df_cities_ibge['city_name'])
  df_cities_ibge['city_name'] = [unidecode(x.upper()) for x in df_cities_ibge['city_name']]

  print('Merging the companies cities and IBGE')
  df_cities = pd.merge(df_cities_ibge,df_cities_companies,how='left',on=['uf_name','city_name'])

  # Create table that will hold data for the IBGE cities
  sql_create_cities = '''CREATE TABLE IF NOT EXISTS cities (
    uf_code text,
    uf_name text,
    mesoregion_code text,
    mesoregion_name text,
    microregion_code text,
    microregion_name text,
    cod_municipio text,
    city_code text,
    city_complete_code text,
    city_name text
  );'''

  print('Creating table cities, if not existent')
  cursorDB.execute(sql_create_cities)

  print('Cleaning the table cities')
  cursorDB.execute('DELETE FROM cities')

  print('Adding new cities to the table')
  df_cities.to_sql('cities', con=conDB, if_exists='replace')

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

  print(f'Finished at {datetime.datetime.now()}')
  conDB.close()

create_table_companies_filtered_state(['SC'])
#create_table_cities()
import sqlite3
import datetime
import pandas as pd
import numpy as np
from unidecode import unidecode

db_location = '/media/sf_share/data/out/'
db_name = 'CNPJ_full.db'

def create_table_companies_south():
  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')
  
  conDB = sqlite3.connect(db_location+db_name)
  cursorDB = conDB.cursor()

  print('Creating the table companies_south..')
  sql_create = '''CREATE TABLE IF NOT EXISTS companies_south (
      cnpj text,
      matriz_filial text,
      razao_social text,
      nome_fantasia text,
      cod_nat_juridica text,
      data_inicio_ativ text,
      cnae_fiscal text,
      cep text,
      uf text,
      cod_municipio text,
      municipio text,
      qualif_resp text,
      capital_social text,
      porte text,
      opc_simples text,
      opc_mei text
      );
   '''

  cursorDB.execute(sql_create)

  # Delete existing data in the table
  print('Cleaning the table companies_south')
  sql_delete = 'DELETE FROM companies_south'
  cursorDB.execute(sql_delete)

  # Inserting the data in the table
  print('Inserting data in the table companies_south')

  sql_insert = '''INSERT INTO companies_south SELECT * FROM (SELECT
      cnpj,
      matriz_filial,
      razao_social,
      nome_fantasia,
      cod_nat_juridica,
      data_inicio_ativ,
      cnae_fiscal,
      cep,
      uf,
      cod_municipio,
      municipio,
      qualif_resp,
      capital_social,
      porte,
      opc_simples,
      opc_mei
    FROM empresas WHERE situacao = "02" AND uf IN("PR","SC","RS"));'''

  cursorDB.execute(sql_insert)

  # Creating indexes
  print('Creating indexes..')
  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_cod_municipio', 'companies_south', 'cod_municipio')
  cursorDB.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_nome_municipio', 'companies_south', 'municipio')
  cursorDB.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_cnpj', 'companies_south', 'cnpj')
  cursorDB.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_porte', 'companies_south', 'porte')
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
        --LIMIT 1300
      ) t1 ON t1.cod_municipio = t0.cod_municipio
    --LIMIT 1300
  '''

  print('Getting the cities from the companies\' database')
  df_cities_companies = pd.read_sql_query(sql_select_cities,conDB)
  print(df_cities_companies)
  df_cities_companies.to_csv('cities-companies.csv')

  # Get the cities from the IBGE CSV downloaded
  print('Getting the cities from the IBGE csv')
  df_cities_ibge = pd.read_csv('assets/cities_brazil.csv')


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

  print(f'Finished at {datetime.datetime.now()}')
  conDB.close()

#create_table_companies_south()
create_table_cities()
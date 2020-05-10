import sqlite3
import datetime
import pandas as pd
import numpy as np

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
      t1.municipio,
      t1.uf
    FROM empresas t0
      INNER JOIN (
        SELECT 
          cod_municipio,
          MIN(municipio) As municipio,
          MIN(uf) AS uf
        FROM empresas
          GROUP BY cod_municipio 
        LIMIT 100
      ) t1 ON t1.cod_municipio = t0.cod_municipio
    LIMIT 100
  '''

  print('Getting the cities from the companies\' database')
  #cursorDB.execute(sql_select_cities)
  #print(cursorDB.fetchall())
  
  df_cities = pd.read_sql_query(sql_select_cities,conDB)
  print(df_cities)

  df_cities_ibge = pd.read_csv('assets/cities_brazil.csv')
  print(df_cities_ibge)

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

  #cursorDB.execute(sql_create_cities)

  print(f'Finished at {datetime.datetime.now()}')
  conDB.close()

#create_table_companies_south()
create_table_cities()
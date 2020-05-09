import sqlite3
import datetime
import pandas as pd
import numpy as np

db_location = '/media/sf_share/data/out/'
db_name = 'CNPJ_full.db'

def create_table_companies_south():
  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')
  print('Creating the table companies_south..')
  conDB = sqlite3.connect(db_location+db_name)

  sql = '''CREATE TABLE companies_south 
            AS SELECT
              cnpj text,
              matriz_filial text,
              razao_social text,
              nome_fantasia text,
              cod_nat_juridica text,
              data_inicio_ativ text,
              cnae_fiscal text,
              cep text,
              uf text,
              cod_municipio  text,
              nome_municipio text,
              qualif_resp text,
              capital_social text,
              porte text,
              opc_simples text,
              opc_mei text

            FROM empresas WHERE situacao = "02" AND uf IN("PR","SC","RS")'''

  cursorDB = conDB.cursor()

  cursorDB.execute(sql)

  # Creating indexes
  print('Creating indexes..')
  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_cod_municipio', 'companies_south', 'cod_municipio')
  cursorBD.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_nome_municipio', 'companies_south', 'nome_municipio')
  cursorBD.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_cnpj', 'companies_south', 'cnpj')
  cursorBD.execute(sql_index)

  sql_index = 'CREATE INDEX {} ON {} ({});'.format('ix_porte', 'companies_south', 'porte')
  cursorBD.execute(sql_index)

  print(f'Finished at {datetime.datetime.now()}')

def create_table_cities():
  start_time = datetime.datetime.now()
  print(f'Started at {datetime.datetime.now()}')
  conDB = sqlite3.connect(db_location+db_name)
  # Create table that will hold data for the IBGE cities

  sql_create_cities = '''CREATE TABLE IF NOT EXISTS cities (
    uf_code text,
    uf_name text,
    mesoregion_code text,
    mesoregion_name text,
    microregion_code text,
    microregion_name text,
    city_code text,
    city_complete_code text,
    city_name text
  );'''

  cursorDB = conDB.cursor()

  cursorDB.execute(sql)

  print(f'Finished at {datetime.datetime.now()}')


create_table_companies_south()
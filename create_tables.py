import sqlite3
import datetime

start_time = datetime.datetime.now()


print(f'Started at {datetime.datetime.now()}')

# Create table that will hold data for the IBGE cities

'''CREATE TABLE IF NOT EXISTS projects (
	id integer PRIMARY KEY,
	name text NOT NULL,
	begin_date text,
	end_date text
);'''


conDB = sqlite3.connect('/media/sf_share/data/out/CNPJ_full.db')

sql = ''' CREATE TABLE empresas_sul 
          AS SELECT
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
            nome_municipio,
            qualif_resp,
            capital_social,
            porte,
            opc_simples,
            opc_mei

          FROM empresas WHERE situacao = "02" AND uf IN("PR","SC","RS")'''

cursorDB = conDB.cursor()

cursorDB.execute(sql)

print(f'Finished at {datetime.datetime.now()}')
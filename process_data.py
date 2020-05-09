import numpy as np
import pandas as pd
import sqlite3
import datetime

print(f'Started at {datetime.datetime.now()}')

conDB = sqlite3.connect('/media/sf_share/data/out/CNPJ_full.db')

#sql = 'SELECT cnpj, razao_social, nome_fantasia, situacao, data_inicio_ativ, municipio, cod_municipio, capital_social, porte  FROM empresas_sc WHERE cod_municipio = 8077 ORDER BY capital_social DESC'

sql = 'SELECT * FROM empresas_sc WHERE cod_municipio = 8077'

df = pd.read_sql_query(sql,conDB)

print(df.info())
df['capital_social_fix'] = df['capital_social']/100
print(df)

print(f'Finished at {datetime.datetime.now()}')
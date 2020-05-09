import sqlite3
import datetime
import numpy as np
import pandas as pd

print(f'Started at {datetime.datetime.now()}')

conDB = sqlite3.connect('/media/sf_share/data/out/CNPJ_full.db')

#sql = 'SELECT cod_municipio, municipio, uf FROM empresas GROUP(cod_municipio, municipio, uf) LIMIT 50'


#cursorDB = conDB.cursor()
df = pd.read_sql_query('SELECT cod_municipio, municipio, uf FROM empresas_sc GROUP BY cod_municipio, municipio, uf LIMIT 3',conDB)

print(df)

print(f'Finished at {datetime.datetime.now()}')
def get_sql_delete(table_name):
  return f'DELETE FROM {table_name};'

def get_sql_create_companies():
  return '''CREATE TABLE IF NOT EXISTS companies (
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
      ); '''

def get_sql_insert_companies(db_short_name,states):
  return f'''INSERT INTO companies SELECT * FROM (SELECT
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
      CAST((capital_social/100) AS INTEGER) AS capital_social, 
      porte, 
      opc_simples, 
      data_opc_simples, 
      data_exc_simples, 
      opc_mei, 
      sit_especial, 
      data_sit_especial
      FROM {db_short_name}.empresas WHERE uf IN({states}));'''

def get_sql_create_cnaes():
  return '''CREATE TABLE IF NOT EXISTS cnaes (
      cnpj text, 
      cnae_ordem integer,
      cnae text);'''

def get_sql_create_cnaes_ibge():
  return '''CREATE TABLE IF NOT EXISTS cnaes_ibge (
    CodigoInt text,
    Codigo text,
    Cnae text,
    CodSecao text,
    Secao text,
    CodDivisao text,
    Divisao text,
    CodGrupo text,
    Grupo text,
    CodClasse text,
    Classe text);'''

def get_sql_select_cities():
  return ''' SELECT DISTINCT 
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
      ) t1 ON t1.cod_municipio = t0.cod_municipio;'''

def get_sql_create_cities():
  return '''CREATE TABLE IF NOT EXISTS cities (
    uf_code text,
    uf_name text,
    mesoregion_code text,
    mesoregion_name text,
    microregion_code text,
    microregion_name text,
    cod_municipio text,
    city_code text,
    city_complete_code text,
    city_name text);'''
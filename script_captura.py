#!/usr/bin/env python
# coding: utf-8

# In[25]:


import pandas as pd
import glob
import http.client
import json
import logging
from importlib import reload
reload(logging)


# In[26]:



#logging.basicConfig(filename='app.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

logging.warning('This is a debug message')


# In[27]:


logging.warning('INICIO DO PROCESSAMENTO')


# # Utils

# In[362]:


AUTHORIZATION="0090ee76-cc91-4956-ab94-616b0ddaa1c9"
SERVER       ="18.219.144.92"

logging.info(f'Utilizando intancia CKAN registrada em {SERVER}')
logging.debug(f'Chave de acesso {AUTHORIZATION}')

conn = http.client.HTTPConnection(SERVER)
logging.debug(conn)


# In[363]:


def send_request(rest_type, endpoint, payload, headers, count=3):
    try:
        logging.info(f"Request: type: {rest_type}, endpoint: {endpoint}")
        logging.debug(payload)
        logging.debug(headers)
        
        conn.request(rest_type, endpoint, payload, headers)
        res = conn.getresponse()
        
        data = res.read().decode("utf-8")
        logging.debug(f"response: {data}")

        return json.loads(data)
    except:
        if count == 0:
            logging.critical(f"Conexao ao endpoint {endpoint} indisponivel")
            raise
        
        logging.warning(f"Conexao ao endpoint {endpoint} com falha! Iniciando nova tentativa ({count} restantes)")
        count = count -1
        return send_request(rest_type, endpoint, payload, headers, count)


# In[364]:


#
# Cria um novo pacote
#
def update_package(data, **kargs):
    payload = json.dumps(data)

    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json'
    }
    
    return send_request("POST", "/api/3/action/package_update", payload, headers, **kargs)


# In[365]:


#
# Retorna o pacote
#
def get_package(package_id: str, **kargs):
    payload = json.dumps({
      "id": package_id
    })

    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json'
    }
    
    return send_request("POST", "/api/3/action/package_show", payload, headers, **kargs)


# In[366]:


#
# Cria um novo pacote
#
def set_package(data, **kargs):
    payload = json.dumps(data)

    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json'
    }
    
    return send_request("POST", "/api/3/action/package_create", payload, headers, **kargs)


# In[367]:


#
# Retorna o pacote
#
def delete_datastore(resource_id: str, **kargs):
    payload = json.dumps({
      "resource_id": resource_id
    })

    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json'
    }
    
    return send_request("POST", "/api/3/action/datastore_delete", payload, headers, **kargs)


# In[368]:


#
# Retorna o pacote
#
def get_datastore(resource_id: str, **kargs):
    payload = json.dumps({
      "resource_id": resource_id
    })

    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json'
    }
    
    return send_request("POST", "/api/3/action/datastore_search", payload, headers, **kargs)


# In[369]:


#
# Cria um novo datastore
#
def set_datastore(data, **kargs):
    payload = json.dumps(data)

    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json'
    }

    return send_request("POST", "/api/3/action/datastore_create", payload, headers, **kargs)


# In[370]:


def create_new_package(tabla):
    package = {
        "name": tabla['database'],
        "title": tabla['database'],
        "owner_org": "bank",
        "notes": "Base carregada por processo automatico",
        "author": "dorotheu",
        "author_email": "dorotheu@teste.com",
        "private": True,
        "extras": [
            {
                "key": "badge",
                "value": "gold"
            },
            {
                "key": "last_modified",
                "value": tabla['last_modified']
            },
            {
                "key": "path",
                "value": tabla['path']
            },
            {
                "key": "import_flag",
                "value": "{}-{}-{}".format(tabla['year'], tabla['month'], tabla['day'])
            }
        ]
    }

    return set_package(package)


# In[371]:


def create_new_datastore(tabla):
    datastore = {
        "resource": {
            "package_id": tabla['database'],
            "name": tabla['database'],
            "description": "Dicionario de Dados",
            "format": "Redshift"
        },
        "fields": tabla['columns'],
    }
    
    return set_datastore(datastore)


# In[372]:


#
def read_partials(key, **kargs):
    all_files = glob.glob(key)
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, **kargs)
        li.append(df)

    return pd.concat(li, axis=0, ignore_index=True)


# In[373]:


def index_out(data, key = 'id'):
    aux = {}
    for item in data:
        aux[item[key]] = item
    
    return aux


# In[374]:


def index_in(data):
    aux = []
    for item in data:
        aux.append(data[item])
        
    return aux


# # Reading

# ### Reading tablas

# In[390]:


tablas = read_partials("tablas*.csv", delimiter=";")
tablas.head()


# ### Reading colunas

# In[391]:


colunas = read_partials("colunas*.csv", delimiter=";")
colunas.head()


# # Merging

# In[392]:


tablas_cols = tablas.merge(colunas, left_on=['database', 'year', 'month', 'day'], right_on=['database', 'year', 'month', 'day'], suffixes=('_left', '_right'))
tablas_cols.head(100)


# ### Lendo apenas as bases de interesse

# In[393]:


tablas_inspect = ['tb_raw_mydatabase_01', 'tb_raw_mydatabase_02', 'tb_raw_mydatabase_03']
tablas_inspect    = tablas_cols.loc[tablas_cols.database.isin(tablas_inspect)]
tablas_inspect.head()


# In[394]:


df_tablas = tablas_inspect


# ### Lendo os pacotes ja carregados no CKAN

# In[395]:


# Lendo as bases ja existentes do catalogo

databases      = df_tablas.database.unique().tolist()
databases_aux  = df_tablas.database.unique().tolist()
ckan_databases = {}

for database in databases_aux:
    response = get_package(database)
    if "success" in response and response["success"] == True:
        logging.info(f"Tabla {database} ja cadastrada no ckan!")
        ckan_databases[database] = response["result"]
        databases.remove(database)


# # Bases novas

# ### Isolando as bases que ainda nao foram carregadas

# In[396]:


# Para as bases que ainda nao estao inclusas no catalogo
tablas_nuevas = df_tablas.loc[~df_tablas.database.isin(ckan_databases.keys())]


# ### Criando o objeto de pacotes

# In[397]:


# Der as tabelas disponiveis
my_tablas = {}
for tabla in databases:
    my_tablas[tabla] = {}
    
    db = df_tablas.loc[(df_tablas['database'] == tabla)]                    .sort_values(by=['year', 'month', 'day'], ascending=False)                    .reset_index(drop=True)
    
    data = json.loads(db.to_json())

    for key in data.keys():
        my_tablas[tabla][key] = data[key]['0']

logging.debug(f"Nuevas tablas: {my_tablas}")


# ### Carregando as colunas dentro do objeto de pacotes

# In[398]:


# Lendo as colunas das tabelas disponiveis
for tabla_name in my_tablas:
    db = df_tablas.loc[(df_tablas['database'] == tabla_name)]                    .loc[(df_tablas['year']  == my_tablas[tabla_name]['year'])]                    .loc[(df_tablas['month'] == my_tablas[tabla_name]['month'])]                    .loc[(df_tablas['day']   == my_tablas[tabla_name]['day'])]                    .reset_index(drop=True)
    
    my_tablas[tabla_name]['columns'] = []

    data = json.loads(db.to_json())
    size = len(db.index)
    
    #for row in size:
    for row in range(0, size):
        my_tablas[tabla_name]['columns'].append({
            "id": data['column'][f"{row}"],
            "type": data['type'][f"{row}"],
            "info": {
                "label": data['column'][f"{row}"],
                "notes": data['comment'][f"{row}"]
            }
        })

logging.debug(f"Nuevas tablas (com culunas): {my_tablas}")


# In[399]:


for tabla_name in my_tablas:
    response_package   = create_new_package(my_tablas[tabla_name])
    response_datastore = create_new_datastore(my_tablas[tabla_name])


# # Atualizando pacotes

# Das bases ja carregadas, carregar as alteracoes

# In[400]:


df_tablas.head()


# In[401]:


import_flag = ['2021', '6', '1']
df_tablas['flag'] = df_tablas['year'].astype(str) + df_tablas['month'].astype(str) + df_tablas['day'].astype(str)
db = df_tablas.loc[(df_tablas['database'] == "tb_raw_mydatabase_01")]                    .loc[(df_tablas['flag'] > "{}{}{}".format(import_flag[0], import_flag[1], import_flag[2]))]                    .sort_values(by=['year', 'month', 'day'], ascending=True)                    .reset_index(drop=True)

db.head(10)


# In[402]:


# Der as tabelas disponiveis
my_update_tablas = {}
df_tablas['flag'] = df_tablas['year'].astype(str) + df_tablas['month'].astype(str) + df_tablas['day'].astype(str)

for tabla in ckan_databases:
    import_flag = []
    
    # Le a ultima data carregada do pacote
    for extra in ckan_databases[tabla]['extras']:
        if extra['key'] == "import_flag":
            import_flag = extra['value'].split('-')
            
    # Procurando por atualizacoes nas tabelas de entrada
    db = df_tablas.loc[(df_tablas['database'] == tabla)]                    .loc[(df_tablas['flag'] > "{}{}{}".format(import_flag[0], import_flag[1], import_flag[2]))]                    .sort_values(by=['year', 'month', 'day'], ascending=True)                    .reset_index(drop=True)
    
    # Caso nao exista nenhuma atualizacao, entao o processo e ignorado
    if not len(db.index):
        continue
    
    my_update_tablas[tabla] = {}
    data = json.loads(db.to_json())
    
    
    # Processo de leitura do pacote a partir do arquivo
    # TODO: Processo muito parecido com o de novos pacotes, avaliar reuso
    for key in data.keys():
        my_update_tablas[tabla][key] = data[key]['0']
    

    size = len(db.index)
    my_update_tablas[tabla]['columns'] = []
    
    #for row in size:
    for row in range(0, size):
        my_update_tablas[tabla]['columns'].append({
            "id": data['column'][f"{row}"],
            "type": data['type'][f"{row}"],
            "info": {
                "label": data['column'][f"{row}"],
                "notes": data['comment'][f"{row}"]
            }
        })
    
    # Carregando o datastore dentro do pacote (ckan) para mesclagem
    for res in ckan_databases[tabla]['resources']:
        if res['format'] == "Redshift":
            response = get_datastore(res["id"])
            if response['success'] == True:
                ckan_databases[tabla]['resource_data'] = response['result']
        
    
    # Transformacao das colunas em chave, utilizada no processo de merge
    my_columns   = index_out(my_update_tablas[tabla]['columns'])
    ckan_columns = index_out(ckan_databases[tabla]['resource_data']['fields'])

    for col in ckan_columns.keys():
        if col in my_columns:
            # Campos que sao considerados exclusivamente do front (force) devem ser especificados aqui
            my_columns[col]['info']['notes'] = ckan_columns[col]['info']['notes']
    
    # Retornando os campos para o formato original
    ckan_databases[tabla]['resource_data']['fields'] = index_in(my_columns)
    
    # Deletando o datastore existente
    res = delete_datastore(ckan_databases[tabla]['resource_data']['resource_id'])
    logging.debug(f"Deletando recurso: {res}")
    
    # Criando um novo datastore com a mesclagem de campos
    # Levando em consideracao a seguinte regra:
    #     - Campos novos encontrados na origem irao ser incluidos no ckan
    #     - Campos novos do ckan nao encontrados na origem serao removidos
    #     - Campos do ckan e da origem iguais serao permanecidos, porem e considerado
    #         o label definido no ckan
    res = set_datastore(ckan_databases[tabla]['resource_data'])
    logging.debug(f"Criando recurso: {res}")
    
    # Update da tabela de pacotes
    for extra in ckan_databases[tabla]['extras']:
        if extra['key'] == "import_flag":
            extra['value'] = "{}-{}-{}".format(
                my_update_tablas[tabla]['year'],
                my_update_tablas[tabla]['month'],
                my_update_tablas[tabla]['day']
            )
           
    res = update_package(ckan_databases[tabla])
    logging.debug(f"Atualizando flag do pacote: {res}")


# In[ ]:





# In[ ]:





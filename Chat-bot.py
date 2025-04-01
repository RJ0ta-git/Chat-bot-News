import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import json

load_dotenv()

api_news = os.getenv("API_NEWSAPI")
db_name = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = 5432
host = os.getenv("host")
schema_name = "01-Bronze"
table_name = "chat-bot-base"

def get_news(category, api_key):
    url = f"https://newsapi.org/v2/top-headlines?category={category}&apiKey={api_key}"
    print(url)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

news_data = get_news("technology", api_news)
df = None

if news_data and news_data['status'] == 'ok' and 'articles' in news_data:
    # Converta a lista de artigos em um DataFrame
    df = pd.DataFrame(news_data['articles'])

    # Converta a coluna 'source' para string JSON
    df['source'] = df['source'].apply(json.dumps)

    print("DataFrame criado com sucesso:")
    print(df.head()) # Imprima as primeiras linhas para verificar a transformação
else:
    print("Erro ao obter os dados da API ou a chave 'articles' não foi encontrada.")

def data_input(table_name, schema_name, df, user, password, host, port, db_name):
    if df is not None:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

        try:
            df.to_sql(name=table_name,
                      con=engine,
                      schema=schema_name,
                      if_exists='append',
                      index=False)
            print(f"DataFrame inserido com sucesso na tabela '{schema_name}.{table_name}'.")
        except Exception as e:
            print(f"Erro ao inserir o DataFrame no PostgreSQL: {e}")
        finally:
            engine.dispose()
    else:
        print("Não há DataFrame para inserir no banco de dados.")

if df is not None:
    data_input(table_name, schema_name, df, user, password, host, port, db_name)

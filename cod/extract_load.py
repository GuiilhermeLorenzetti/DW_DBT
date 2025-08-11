# import 

import pandas as pd 
import yfinance
from sqlalchemy import create_engine
from dotenv import load_dotenv 
import os


# import variaveis de ambiente

commodities = ['CL=F', 'GC=F', 'SI=F']

DB_HOST_PROD = os.getenv('DB_HOST_PROD')
DB_PORT_PROD = os.getenv('DB_PORT_PROD')
DB_NAME_PROD = os.getenv('DB_NAME_PROD')
DB_USER_PROD = os.getenv('DB_USER_PROD')
DB_PASS_PROD = os.getenv('DB_PASS_PROD')
DB_SCHEMA_PROD = os.getenv('DB_SCHEMA_PROD')

def buscar_dados_commodities(simbolo, periodo= '5d', intervalo= '1d'):
    ticker = yfinance.Ticker(simbolo)
    dados = ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

def  concatenar_dados_commodities(commodities):
    dados_concatenados = pd.DataFrame()
    for simbolo in commodities:
        dados = buscar_dados_commodities(simbolo)
        dados_concatenados = pd.concat([dados_concatenados, dados])
    return dados_concatenados


def salvar_dados_no_banco_de_dados(dados_concatenados):
    engine = create_engine(os.getenv('DATABASE_URL'))
    dados_concatenados.to_sql('dados_commodities', engine, if_exists='replace', index=True, index_label='date')

get_data = concatenar_dados_commodities(commodities)
salvar_dados_no_banco_de_dados(get_data)









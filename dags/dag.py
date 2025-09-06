from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import json
import time
import logging
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, ChunkedEncodingError
import zipfile
import smtplib
import ssl
from email.message import EmailMessage
import mimetypes
from utils.clima import coords_por_municipio, clima_diario_por_municipio
logging.getLogger("airflow.hooks.base").setLevel(logging.ERROR)

SUBE_LIMIT = 1000
OUTPUT_PATH = "/tmp/pokemon_data/pokemon_base.csv"
SUBE_DATA_PATH = "/tmp/sube_data/sube_data.csv"
MERGED_DATA_PATH = "/tmp/pokemon_data/pokemon_merged.csv"
SUBE_URL_CSV = "/upload/Dat_Ab_Usos/dat-ab-usos-2024.csv"
LOCAL_SUBE = "/tmp/dat-ab-usos-2024.csv"
LOCAL_FERIADOS = "/tmp/feriados/feriados_2024.json"
CHUNK = 1024 * 1024  # 1 MiB
ANIO = 2024

default_args = {
    'owner': 'grupo 17',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'depends_on_past': False,
}

def download_sube_csv(**kwargs):
    
    base = "https://archivos-datos.transporte.gob.ar"
    url = base + SUBE_URL_CSV

    os.makedirs(os.path.dirname(LOCAL_SUBE), exist_ok=True)

    if os.path.exists(LOCAL_SUBE):
        return

    try:
        # Intenta obtener el archivo sin reintentos
        with requests.get(url, stream=True, timeout=(10, 300)) as r:
            r.raise_for_status()
            with open(LOCAL_SUBE, "wb") as f:
                for chunk in r.iter_content(chunk_size=CHUNK):
                    if chunk:
                        f.write(chunk)    
        
        return

    except (ConnectionError, HTTPError, ReadTimeout, ChunkedEncodingError) as e:
        print(f"Error al descargar el archivo: {e}")
        return
    
def download_feriados(**kwargs):

    url = "https://api.argentinadatos.com/v1/feriados/2024"

    os.makedirs(os.path.dirname(LOCAL_FERIADOS), exist_ok=True)

    if os.path.exists(LOCAL_FERIADOS):
        return

    try:
        r = requests.get(url, timeout=(10, 30))
        r.raise_for_status()
        data = r.json()  # parsear JSON directo
        with open(LOCAL_FERIADOS, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return LOCAL_FERIADOS

    except (ConnectionError, HTTPError, ReadTimeout, ChunkedEncodingError) as e:
        print(f"Error al descargar el archivo: {e}")
        raise

# Tarea A: /pokemon/{id}
def extract_sube(**kwargs):
    
    df = pd.read_csv(LOCAL_SUBE)

    df['DIA_TRANSPORTE'] = pd.to_datetime(df['DIA_TRANSPORTE'], format='%Y-%m-%d', errors='coerce')

    columns = df.columns.tolist()

    # Preparo un csv para obtener los datos del clima posteriormente

    # Renombro las columnas
    df = df.rename(columns={
        "DIA_TRANSPORTE": "fecha",
        "PROVINCIA": "provincia",
        "MUNICIPIO": "municipio",
        "NOMBRE_EMPRESA": "empresa",
        "LINEA": "linea",
        "AMBA": "amba",
        "TIPO_TRANSPORTE": "tipo_transporte",
        "JURISDICCION": "jurisdiccion",
        "CANTIDAD": "cantidad",
        "DATO_PRELIMINAR": "dato_preliminar"})
    
    # Elimino columnas innecesarias
    df = df.drop(columns=["dato_preliminar", "amba", "jurisdiccion"])

    print(columns)

    return

def extract_feriados(**kwargs):
    
    df = pd.read_json(LOCAL_FERIADOS)

    columns = df.columns.tolist()

    print(columns)

    return

# DAG
with DAG(
    dag_id='pokemon_base_etl_parallel',
    description='DAG ETL paralelo que une data de /pokemon y /pokemon-species',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['pokemon', 'parallel', 'etl']
) as dag:

    # Acá se hacen las tareas  NO FUNCIONA PORQUE ES MUY GRANDE EL CSV
    # fetch_sube_data = HttpOperator(
    #     task_id='fetch_sube_data',
    #     http_conn_id='subeapi',
    #     endpoint=SUBE_CSV_URL,
    #     method='GET',
    #     log_response=True,
    #     response_filter=lambda response: response.text,
    #     do_xcom_push=True,
    # )

    download_sube = PythonOperator(
        task_id="download_sube_csv",
        python_callable=download_sube_csv,
    )

    download_feriados = PythonOperator(
        task_id="download_feriados",
        python_callable=download_feriados,
    )

    sube_extract = PythonOperator(
        task_id='sube_extract_data',
        python_callable=extract_sube,
    )

    feriados_extract = PythonOperator(
        task_id='extract_feriados',
        python_callable=extract_feriados,
    )

    download_sube >> download_feriados >> [sube_extract, feriados_extract]

    # fetch_sube_data >> [download_a, download_b] >> merge_transform >> export_logs >> send_email

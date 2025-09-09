from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests, os, json, zipfile, time
import unicodedata
import re
from math import isnan

# -----------------------
# Configuración
# -----------------------
SUBE_URL = "https://archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos/dat-ab-usos-2024.csv"
LOCAL_SUBE = "/usr/local/airflow/logs/data/dat-ab-usos-2024.csv"
LOCAL_FERIADOS = "/usr/local/airflow/logs/data/feriados_2024.json"
LOCAL_COORDS = "/usr/local/airflow/logs/data/municipios_coords.csv"
OUTPUT_DIR = "/usr/local/airflow/logs/data/output"
CHUNK = 1024 * 1024

default_args = {
    "owner": "grupo17",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# -----------------------
# Funciones
# -----------------------

def download_sube_csv(**kwargs):
    os.makedirs(os.path.dirname(LOCAL_SUBE), exist_ok=True)
    if os.path.exists(LOCAL_SUBE):
        return LOCAL_SUBE
    r = requests.get(SUBE_URL, stream=True, timeout=(10, 300))
    r.raise_for_status()
    with open(LOCAL_SUBE, "wb") as f:
        for chunk in r.iter_content(chunk_size=CHUNK):
            if chunk:
                f.write(chunk)
    return LOCAL_SUBE

def download_feriados(**kwargs):
    url = "https://api.argentinadatos.com/v1/feriados/2024"
    os.makedirs(os.path.dirname(LOCAL_FERIADOS), exist_ok=True)
    if os.path.exists(LOCAL_FERIADOS):
        return LOCAL_FERIADOS
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with open(LOCAL_FERIADOS, "w", encoding="utf-8") as f:
        json.dump(r.json(), f, indent=2, ensure_ascii=False)
    return LOCAL_FERIADOS

def fetch_all_municipios(**kwargs):
    """Obtiene coordenadas de todos los municipios de Argentina"""
    url = "https://apis.datos.gob.ar/georef/api/municipios"
    params = {
        "aplanar": "true",
        "max": 5000,
        "inicio": 0
    }
    frames = []
    total = None
    
    while True:
        r = requests.get(url, params={k:v for k,v in params.items() if v is not None}, timeout=30)
        r.raise_for_status()
        js = r.json()
        if total is None:
            total = js.get("total")
            print(f"Esperados (total): {total}")
        munis = js.get("municipios", [])
        if not munis:
            break
        frames.append(pd.DataFrame(munis))
        params["inicio"] += len(munis)
        print(f"Acumulados: {params['inicio']}")
        if params["inicio"] >= total:
            break
        time.sleep(0.15)  # cortesía
    
    df = pd.concat(frames, ignore_index=True)
    
    # Renombrar columnas
    df = df.rename(columns={
        "nombre": "municipio",
        "provincia_nombre": "provincia",
        "centroide_lat": "lat",
        "centroide_lon": "lon"
    })[["provincia","municipio","lat","lon"]]
    
    # Guardar archivo
    os.makedirs(os.path.dirname(LOCAL_COORDS), exist_ok=True)
    df.to_csv(LOCAL_COORDS, index=False)
    print(f"Coordenadas guardadas: {df.shape[0]} municipios")
    return LOCAL_COORDS

def quitar_tildes(texto):
    """Normaliza texto removiendo tildes y acentos"""
    if texto is None or pd.isna(texto):
        return None
    
    # Normaliza (NFKD = compatibilidad, descompone caracteres con tilde)
    nfkd = unicodedata.normalize("NFKD", texto)
    # Elimina marcas de acento
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def extract_sube(**kwargs):
    df = pd.read_csv(LOCAL_SUBE)
    df["DIA_TRANSPORTE"] = pd.to_datetime(df["DIA_TRANSPORTE"], errors="coerce")
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
        "DATO_PRELIMINAR": "dato_preliminar"
    })
    # Eliminar columnas como en Colab
    df = df.drop(columns=["dato_preliminar", "amba", "jurisdiccion"])
    
    # FILTRAR SOLO UN DÍA ESPECÍFICO
    target_date = "2024-01-15"  # Cambia esta fecha si quieres otro día
    df = df[df["fecha"].dt.date == pd.to_datetime(target_date).date()]
    
    out = f"{OUTPUT_DIR}/sube_extract_single_day.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"SUBE filtrado para {target_date}: {df.shape[0]} registros")
    return out

def extract_feriados(**kwargs):
    df = pd.read_json(LOCAL_FERIADOS)
    df = df.rename(columns={"fecha": "fecha_feriado"})
    out = f"{OUTPUT_DIR}/feriados_extract.csv"
    df.to_csv(out, index=False)
    return out

def merge_coordinates(**kwargs):
    """Merge de coordenadas con datos SUBE"""
    ti = kwargs["ti"]
    sube_path = ti.xcom_pull(task_ids="extract_sube")
    
    # Leer datos SUBE (ya filtrado por un día)
    df_sube = pd.read_csv(sube_path)
    
    # Obtener municipios únicos
    input_coord = df_sube[['provincia', 'municipio']].drop_duplicates().reset_index(drop=True)
    
    # Leer coordenadas
    df_coords = pd.read_csv(LOCAL_COORDS)
    
    # Normalizar nombres
    df_coords['municipio'] = df_coords['municipio'].apply(quitar_tildes).str.lower().str.strip()
    df_coords['provincia'] = df_coords['provincia'].apply(quitar_tildes).str.lower().str.strip()
    
    input_coord['municipio'] = input_coord['municipio'].apply(quitar_tildes).str.lower().str.strip()
    input_coord['provincia'] = input_coord['provincia'].apply(quitar_tildes).str.lower().str.strip()
    
    # Merge
    df_merged = pd.merge(
        left=input_coord,
        right=df_coords,
        how="left",
        on=['provincia', 'municipio']
    )
    
    # Eliminar NaNs
    df_merged = df_merged.dropna()
    
    out = f"{OUTPUT_DIR}/municipios_with_coords_single_day.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_merged.to_csv(out, index=False)
    print(f"Municipios con coordenadas: {df_merged.shape[0]}")
    return out

def enrich_with_weather(**kwargs):
    """Obtiene datos de clima para un día específico"""
    ti = kwargs["ti"]
    coords_path = ti.xcom_pull(task_ids="merge_coordinates")
    
    # Parámetros para UN SOLO DÍA
    target_date = "2024-01-15"  # Mismo día que en extract_sube
    tz = "America/Argentina/Mendoza"
    
    # Leer municipios con coordenadas
    df_coords = pd.read_csv(coords_path)
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    rows = []
    
    for i, row in df_coords.iterrows():
        lat = row["lat"]
        lon = row["lon"]
        provincia = row["provincia"]
        municipio = row["municipio"]
        
        try:
            resp = requests.get(
                url,
                params={
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "start_date": target_date,
                    "end_date": target_date,  # Mismo día
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
                    "timezone": tz,
                },
                timeout=30,
            )
            resp.raise_for_status()
            js = resp.json()
            daily = js.get("daily", {})
            if daily and daily.get("time"):
                rows.append({
                    "provincia": provincia,
                    "municipio": municipio,
                    "lat": float(lat),
                    "lon": float(lon),
                    "fecha": daily["time"][0],
                    "tmax": daily["temperature_2m_max"][0],
                    "tmin": daily["temperature_2m_min"][0],
                    "precip": daily["precipitation_sum"][0],
                    "viento": daily["windspeed_10m_max"][0],
                })
        except Exception as e:
            print(f"ERROR en fila {i} ({provincia} - {municipio}): {e}")
            pass
        
        # Pequeña pausa de cortesía para no martillar la API
        time.sleep(0.05)
    
    df_clima = pd.DataFrame(rows)
    out = f"{OUTPUT_DIR}/weather_single_day_{target_date}.csv"
    df_clima.to_csv(out, index=False)
    print(f"Datos de clima obtenidos para {len(rows)} municipios en {target_date}")
    return out

def merge_and_transform(**kwargs):
    """Merge simple sin chunks"""
    ti = kwargs["ti"]
    sube_path = ti.xcom_pull(task_ids="extract_sube")
    feriados_path = ti.xcom_pull(task_ids="extract_feriados")
    weather_path = ti.xcom_pull(task_ids="enrich_with_weather")
    date = kwargs["ds"]

    # Cargar todos los archivos (ahora son pequeños)
    df_sube = pd.read_csv(sube_path)
    df_fer = pd.read_csv(feriados_path)
    df_weather = pd.read_csv(weather_path)
    
    # Debug: verificar columnas
    print(f"Columnas SUBE: {list(df_sube.columns)}")
    print(f"Columnas feriados: {list(df_fer.columns)}")
    print(f"Columnas weather: {list(df_weather.columns)}")
    
    # Debug: verificar tipos de fecha
    print(f"Tipo fecha SUBE: {df_sube['fecha'].dtype}")
    print(f"Tipo fecha weather: {df_weather['fecha'].dtype}")
    print(f"Tipo fecha feriados: {df_fer['fecha_feriado'].dtype}")
    
    # Convertir fechas a string para el merge
    df_sube['fecha'] = df_sube['fecha'].astype(str)
    df_weather['fecha'] = df_weather['fecha'].astype(str)
    df_fer['fecha_feriado'] = df_fer['fecha_feriado'].astype(str)

    # Primer merge: SUBE con clima por fecha
    df = df_sube.merge(
        df_weather,
        how="left",
        on=['fecha']
    )
    
    # Segundo merge: resultado con feriados por fecha
    df_final = df.merge(
        df_fer,
        how="left",
        left_on=['fecha'],
        right_on=['fecha_feriado']
    )
    
    # Eliminar columnas como en Colab
    df_final = df_final.drop(columns=["tipo", "lat", "lon"])
    df_final["grupo"] = "Grupo 17"

    out = f"{OUTPUT_DIR}/final_single_day_{date}.csv"
    df_final.to_csv(out, index=False)
    print(f"Archivo final creado con {df_final.shape[0]} registros")
    return out

def show_results(**kwargs):
    """Muestra un resumen de los resultados"""
    ti = kwargs["ti"]
    final_path = ti.xcom_pull(task_ids="merge_and_transform")
    
    df = pd.read_csv(final_path)
    
    print(f"\n=== RESUMEN DE RESULTADOS ===")
    print(f"Total de registros: {df.shape[0]}")
    print(f"Columnas: {list(df.columns)}")
    print(f"\nPrimeras 5 filas:")
    print(df.head())
    print(f"\nEstadísticas de clima:")
    print(df[['tmax', 'tmin', 'precip', 'viento']].describe())
    
    return f"Procesados {df.shape[0]} registros exitosamente"

# -----------------------
# Definición del DAG
# -----------------------
with DAG(
    dag_id="simple_tp1_grupo17",
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=["simple","sube","clima","feriados","single_day"]
) as dag:

    t1 = PythonOperator(task_id="download_sube_csv", python_callable=download_sube_csv)
    t2 = PythonOperator(task_id="download_feriados", python_callable=download_feriados)
    t3 = PythonOperator(task_id="fetch_all_municipios", python_callable=fetch_all_municipios)
    t4 = PythonOperator(task_id="extract_sube", python_callable=extract_sube)
    t5 = PythonOperator(task_id="extract_feriados", python_callable=extract_feriados)
    t6 = PythonOperator(task_id="merge_coordinates", python_callable=merge_coordinates)
    t7 = PythonOperator(task_id="enrich_with_weather", python_callable=enrich_with_weather)
    t8 = PythonOperator(task_id="merge_and_transform", python_callable=merge_and_transform)
    t9 = PythonOperator(task_id="show_results", python_callable=show_results)

    # Dependencias
    t1 >> t4  # download_sube_csv -> extract_sube
    t2 >> t5  # download_feriados -> extract_feriados
    t3 >> t6  # fetch_all_municipios -> merge_coordinates
    t4 >> t6  # extract_sube -> merge_coordinates
    t6 >> t7  # merge_coordinates -> enrich_with_weather
    [t4, t5, t7] >> t8 >> t9  # extract_sube, extract_feriados, enrich_with_weather -> merge_and_transform -> show_results

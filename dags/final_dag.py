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
LAT, LON = -34.6, -58.4   # por defecto CABA

default_args = {
    "owner": "grupo17",
    "depends_on_past": False,
    "email_on_failure": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

#Parametros
fecha_inicio = "2024-02-01"
fecha_fin = "2024-02-29"
tz = "America/Argentina/Mendoza"

# -----------------------
# Funciones
# -----------------------

def normalize_text(texto):
    """Normaliza texto de manera consistente en todo el pipeline"""
    if texto is None or pd.isna(texto):
        return None
    
    texto = str(texto).strip()
    
    # Normaliza (NFKD = compatibilidad, descompone caracteres con tilde)
    nfkd = unicodedata.normalize("NFKD", texto)
    # Elimina marcas de acento
    normalized = "".join([c for c in nfkd if not unicodedata.combining(c)])
    
    return normalized.lower()

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
    
    # Normalizar nombres AQUÍ TAMBIÉN
    df['municipio'] = df['municipio'].apply(normalize_text)
    df['provincia'] = df['provincia'].apply(normalize_text)
    
    # Guardar archivo
    os.makedirs(os.path.dirname(LOCAL_COORDS), exist_ok=True)
    df.to_csv(LOCAL_COORDS, index=False)
    print(f"Coordenadas guardadas: {df.shape[0]} municipios")
    return LOCAL_COORDS

def merge_coordinates(**kwargs):
    """Merge de coordenadas con datos SUBE"""
    ti = kwargs["ti"]
    sube_path = ti.xcom_pull(task_ids="extract_sube")
    
    # Leer datos SUBE completos (solo necesitamos municipios únicos)
    df_sube = pd.read_csv(sube_path)
    
    # Obtener municipios únicos
    input_coord = df_sube[['provincia', 'municipio']].drop_duplicates().reset_index(drop=True)
    
    # Leer coordenadas
    df_coords = pd.read_csv(LOCAL_COORDS)
    
    # Normalizar nombres usando la función consistente
    input_coord['municipio_norm'] = input_coord['municipio'].apply(normalize_text)
    input_coord['provincia_norm'] = input_coord['provincia'].apply(normalize_text)
    
    # Los datos de coordenadas ya están normalizados
    
    # Merge
    df_merged = pd.merge(
        left=input_coord,
        right=df_coords,
        how="left",
        left_on=['provincia_norm', 'municipio_norm'],
        right_on=['provincia', 'municipio']
    )
    
    # Mantener las columnas originales y las coordenadas
    df_merged = df_merged[['provincia_x', 'municipio_x', 'lat', 'lon']].rename(columns={
        'provincia_x': 'provincia',
        'municipio_x': 'municipio'
    })
    
    # Eliminar NaNs
    df_merged = df_merged.dropna()
    
    out = f"{OUTPUT_DIR}/municipios_with_coords.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_merged.to_csv(out, index=False)
    print(f"Municipios con coordenadas: {df_merged.shape[0]}")
    return out

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

    df["fecha"] = pd.to_datetime(df['fecha'], errors="coerce")

    # Eliminar columnas como en Colab
    df = df.drop(columns=["dato_preliminar", "amba", "jurisdiccion"])
    out = f"{OUTPUT_DIR}/sube_extract.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(out, index=False)
    return out

def extract_feriados(**kwargs):
    df = pd.read_json(LOCAL_FERIADOS)
    df = df.rename(columns={"fecha": "fecha"})

    out = f"{OUTPUT_DIR}/feriados_extract.csv"
    df.to_csv(out, index=False)
    return out

def enrich_with_weather(**kwargs):
    """Obtiene datos de clima para todos los municipios con coordenadas"""
    ti = kwargs["ti"]
    coords_path = ti.xcom_pull(task_ids="merge_coordinates")
    
    # Parámetros como en el código de Colab
    fecha_inicio = "2024-01-01"
    fecha_fin = "2024-12-31"
    tz = "America/Argentina/Mendoza"
    
    # Leer municipios con coordenadas
    df_coords = pd.read_csv(coords_path)
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    rows = []
    
    for i, row in df_coords.iterrows():
        lat = row["lat"]
        lon = row["lon"]
        provincia = row["provincia"]  # YA ESTÁN NORMALIZADOS
        municipio = row["municipio"]  # YA ESTÁN NORMALIZADOS
        
        try:
            resp = requests.get(
                url,
                params={
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "start_date": fecha_inicio,
                    "end_date": fecha_fin,
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
                    "timezone": tz,
                },
                timeout=30,
            )
            resp.raise_for_status()
            js = resp.json()

            daily = js.get("daily", {})

            if daily and daily.get("time"):
                for fecha, tmax, tmin, prec, viento in zip(
                    daily["time"],
                    daily.get("temperature_2m_max", []),
                    daily.get("temperature_2m_min", []),
                    daily.get("precipitation_sum", []),
                    daily.get("windspeed_10m_max", []),
                ):
                    rows.append({
                        "provincia": provincia,  # Ya normalizado
                        "municipio": municipio,  # Ya normalizado
                        "lat": float(lat),
                        "lon": float(lon),
                        "fecha": fecha,     
                        "tmax": tmax,
                        "tmin": tmin,
                        "precip": prec,
                        "viento": viento,
                    })
       
        except Exception as e:
            print(f"ERROR en fila {i} ({provincia} - {municipio}): {e}")
            pass
        
        # Pequeña pausa de cortesía para no martillar la API
        time.sleep(0.05)
    
    df_clima = pd.DataFrame(rows)
    df_clima['fecha'] = pd.to_datetime(df_clima['fecha'])

    out = f"{OUTPUT_DIR}/weather_municipios_2024.csv"
    df_clima.to_csv(out, index=False)
    print(f"Datos de clima obtenidos para {len(rows)} registros de {df_coords.shape[0]} municipios")
    return out

def merge_and_transform(**kwargs):
    import duckdb

    ti = kwargs["ti"]
    sube_path     = ti.xcom_pull(task_ids="extract_sube")
    feriados_path = ti.xcom_pull(task_ids="extract_feriados")
    weather_path  = ti.xcom_pull(task_ids="enrich_with_weather")
    date = kwargs["ds"]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = f"{OUTPUT_DIR}/final_{date}.csv"

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # CREAR FUNCIÓN DE NORMALIZACIÓN EN DUCKDB
    con.execute("""
    CREATE OR REPLACE FUNCTION normalize_text(text_val) AS (
        CASE 
            WHEN text_val IS NULL THEN NULL
            ELSE LOWER(TRIM(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    text_val, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ü', 'u'), 'ñ', 'n'), 'Ñ', 'n')
            ))
        END
    );
    """)

    # Hacer el merge con normalización consistente
    con.execute(f"""
        COPY (
        SELECT
            s.*,
            w.tmax, w.tmin, w.precip, w.viento,
            CASE WHEN f.fecha IS NOT NULL THEN 1 ELSE 0 END AS is_feriado,
            f.tipo AS tipo_feriado,
            f.nombre AS nombre_feriado
        FROM read_csv_auto('{sube_path}', SAMPLE_SIZE=-1) AS s
        LEFT JOIN read_csv_auto('{weather_path}', SAMPLE_SIZE=-1) AS w
            ON CAST(s.fecha AS DATE) = TRY_CAST(w.fecha AS DATE)
            AND normalize_text(s.provincia) = normalize_text(w.provincia)
            AND normalize_text(s.municipio) = normalize_text(w.municipio)
        LEFT JOIN read_csv_auto('{feriados_path}', SAMPLE_SIZE=-1) AS f
            ON CAST(s.fecha AS DATE) = TRY_CAST(f.fecha AS DATE)
        )
        TO '{out}' (HEADER, DELIMITER ',');
    """)
    print(f"DuckDB: archivo final creado en {out}")

    # Debug: ¿Cuántos match reales hay?
    matches = con.execute(f"""
    SELECT COUNT(*) as matches
    FROM read_csv_auto('{sube_path}') s
    JOIN read_csv_auto('{weather_path}') w
        ON CAST(s.fecha AS DATE) = TRY_CAST(w.fecha AS DATE)
        AND normalize_text(s.provincia) = normalize_text(w.provincia)
        AND normalize_text(s.municipio) = normalize_text(w.municipio)
    """).fetchall()
    print(f"Matches encontrados: {matches}")

    # Muéstrame algunos registros para inspeccionar
    sample_weather = con.execute(f"""
    SELECT DISTINCT provincia, municipio 
    FROM read_csv_auto('{weather_path}') 
    LIMIT 10
    """).df()
    print("Sample weather data:")
    print(sample_weather)

    sample_sube = con.execute(f"""
    SELECT DISTINCT provincia, municipio 
    FROM read_csv_auto('{sube_path}') 
    LIMIT 10
    """).df()
    print("Sample SUBE data:")
    print(sample_sube)

    return out

def export_logs_zip(**kwargs):
    date = kwargs["ds"]
    dag_id = kwargs["dag"].dag_id
    logs_dir = f"/usr/local/airflow/logs/dag_id={dag_id}"
    out = f"{OUTPUT_DIR}/logs_{date}.zip"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with zipfile.ZipFile(out, "w") as zf:
        for root, _, files in os.walk(logs_dir):
            for file in files:
                fp = os.path.join(root, file)
                zf.write(fp, arcname=os.path.relpath(fp, logs_dir))
    return out

# -----------------------
# Definición del DAG
# -----------------------
with DAG(
    dag_id="tp1_grupo17",
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=["entrega","sube","clima","feriados"]
) as dag:

    t1 = PythonOperator(task_id="download_sube_csv", python_callable=download_sube_csv)
    t2 = PythonOperator(task_id="download_feriados", python_callable=download_feriados)
    t3 = PythonOperator(task_id="fetch_all_municipios", python_callable=fetch_all_municipios)
    t4 = PythonOperator(task_id="extract_sube", python_callable=extract_sube)
    t5 = PythonOperator(task_id="extract_feriados", python_callable=extract_feriados)
    t6 = PythonOperator(task_id="merge_coordinates", python_callable=merge_coordinates)
    t7 = PythonOperator(task_id="enrich_with_weather", python_callable=enrich_with_weather)
    t8 = PythonOperator(task_id="merge_and_transform", python_callable=merge_and_transform)
    t9 = PythonOperator(task_id="export_logs_zip", python_callable=export_logs_zip)

    # Dependencias
    t1 >> t4  # download_sube_csv -> extract_sube
    t2 >> t5  # download_feriados -> extract_feriados
    t3 >> t6  # fetch_all_municipios -> merge_coordinates
    t4 >> t6  # extract_sube -> merge_coordinates
    t6 >> t7  # merge_coordinates -> enrich_with_weather
    [t4, t5, t7] >> t8 >> t9  # extract_sube, extract_feriados, enrich_with_weather -> merge_and_transform -> export_logs_zip
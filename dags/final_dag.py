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
SUBE_YEARS = (2023, 2024)
SUBE_URL_TEMPLATE = "https://archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos/dat-ab-usos-{year}.csv"
FERIADOS_URL_TEMPLATE = "https://api.argentinadatos.com/v1/feriados/{year}"
LOCAL_DATA_DIR = "/usr/local/airflow/logs/data"
LOCAL_SUBE_TEMPLATE = f"{LOCAL_DATA_DIR}/dat-ab-usos-{{year}}.csv"
LOCAL_FERIADOS_TEMPLATE = f"{LOCAL_DATA_DIR}/feriados_{{year}}.json"
LOCAL_COORDS = f"{LOCAL_DATA_DIR}/municipios_coords.csv"
OUTPUT_DIR = f"{LOCAL_DATA_DIR}/output"
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

# --- helper seguro por si hay NaN ---

def _safe_norm(x):
    try:
        return normalize_text(x) if isinstance(x, str) else x
    except Exception:
        return x

def fetch_province_centroids():
    """Devuelve DataFrame con centroides por provincia (normalizada)."""
    url = "https://apis.datos.gob.ar/georef/api/provincias"
    params = {"aplanar": "true", "max": 100}
    
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
        provs = js.get("provincias", [])
        
        dfp = pd.DataFrame(provs).rename(columns={
            "nombre": "provincia",
            "centroide_lat": "prov_lat",
            "centroide_lon": "prov_lon",
        })[["provincia", "prov_lat", "prov_lon"]]
        
        # Normalizar y convertir coordenadas
        dfp["provincia"] = dfp["provincia"].apply(_safe_norm)
        dfp['prov_lat'] = pd.to_numeric(dfp['prov_lat'], errors='coerce')
        dfp['prov_lon'] = pd.to_numeric(dfp['prov_lon'], errors='coerce')
        
        # Verificar que tengamos todas las provincias con coordenadas válidas
        invalid_coords = dfp[dfp['prov_lat'].isna() | dfp['prov_lon'].isna()]
        if not invalid_coords.empty:
            print(f"ADVERTENCIA: Provincias sin coordenadas válidas: {invalid_coords['provincia'].tolist()}")
        
        print(f"Centroides provinciales obtenidos: {len(dfp)} provincias")
        return dfp
        
    except Exception as e:
        print(f"ERROR obteniendo centroides provinciales: {e}")
        # Fallback con algunas provincias principales manualmente
        fallback_data = [
            {"provincia": "buenos aires", "prov_lat": -36.6769, "prov_lon": -60.5588},
            {"provincia": "ciudad autonoma de buenos aires", "prov_lat": -34.6118, "prov_lon": -58.3960},
            {"provincia": "cordoba", "prov_lat": -31.4135, "prov_lon": -64.1811},
            {"provincia": "santa fe", "prov_lat": -30.7016, "prov_lon": -60.9478},
            {"provincia": "mendoza", "prov_lat": -34.6297, "prov_lon": -68.5816},
        ]
        return pd.DataFrame(fallback_data)



def fetch_all_municipios(**kwargs):
    """Obtiene coordenadas de todos los municipios de Argentina y corrige casos especiales."""

    url = "https://apis.datos.gob.ar/georef/api/municipios"
    params = {"aplanar": "true", "max": 5000, "inicio": 0}
    frames, total = [], None

    while True:
        r = requests.get(url, params={k: v for k, v in params.items() if v is not None}, timeout=30)
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

    # Renombrar
    df = df.rename(columns={
        "nombre": "municipio",
        "provincia_nombre": "provincia",
        "centroide_lat": "lat",
        "centroide_lon": "lon"
    })[["provincia", "municipio", "lat", "lon"]]

    # Normalizaciones (a salvo de NaN)
    df["municipio"] = df["municipio"].apply(_safe_norm)
    df["provincia"] = df["provincia"].apply(_safe_norm)

    # --- traer centroides de provincia y mergear ---
    prov_df = fetch_province_centroids()
    df = df.merge(prov_df, on="provincia", how="left")

    # Verificar coordenadas faltantes ANTES de aplicar casos especiales
    print(f"Municipios sin coordenadas antes de corrección: {df[['lat', 'lon']].isna().any(axis=1).sum()}")

    # --- lista de municipios a "forzar" al centroide provincial ---
    especiales_raw = [
        "SN", "SD", "URBANO DE LA COSTA",  # más NaN
        "RIO GALLEGOS", "RIO GRANDE", "SAN NICOLAS DE LOS ARROYOS",
        "CORONEL ROSALES", "USHUAIA", "GUALEGUAYCHU", "LA BANDA",
    ]
    especiales_norm = set(_safe_norm(x) for x in especiales_raw)

    # CORRECCIÓN 1: Verificar que las coordenadas sean válidas numéricamente
    # Algunas coordenadas pueden ser strings o valores inválidos
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')

    # CORRECCIÓN 2: máscara más completa para identificar coordenadas faltantes
    mask_coordenadas_faltantes = (
        df["lat"].isna() | 
        df["lon"].isna() | 
        (df["lat"] == 0) |  # coordenadas 0,0 suelen ser errores
        (df["lon"] == 0) |
        df["municipio"].isna() | 
        df["municipio"].isin(especiales_norm)
    )

    print(f"Municipios que necesitan corrección: {mask_coordenadas_faltantes.sum()}")

    # CORRECCIÓN 3: Verificar que tengamos coordenadas provinciales válidas
    print(f"Municipios sin coordenadas provinciales: {df['prov_lat'].isna().sum()}")

    # reemplazo: coordenadas = centroide de la provincia
    # Solo reemplazar si tenemos coordenadas provinciales válidas
    mask_provincia_valida = df['prov_lat'].notna() & df['prov_lon'].notna()
    mask_final = mask_coordenadas_faltantes & mask_provincia_valida

    df.loc[mask_final, "lat"] = df.loc[mask_final, "prov_lat"]
    df.loc[mask_final, "lon"] = df.loc[mask_final, "prov_lon"]

    print(f"Coordenadas reemplazadas: {mask_final.sum()}")

    # CORRECCIÓN 4: Para casos donde no tenemos coordenadas provinciales, usar CABA por defecto
    mask_sin_coords = df["lat"].isna() | df["lon"].isna()
    if mask_sin_coords.any():
        print(f"Municipios que quedan sin coordenadas, usando CABA por defecto: {mask_sin_coords.sum()}")
        df.loc[mask_sin_coords, "lat"] = LAT   # -34.6 (CABA)
        df.loc[mask_sin_coords, "lon"] = LON   # -58.4 (CABA)

    # limpieza columnas auxiliares
    df = df.drop(columns=["prov_lat", "prov_lon"])

    # Verificación final
    coords_faltantes_final = df[['lat', 'lon']].isna().any(axis=1).sum()
    print(f"Municipios sin coordenadas después de corrección: {coords_faltantes_final}")

    # Mostrar algunos ejemplos de lo que se corrigió
    if mask_final.any():
        print("Ejemplos de municipios corregidos:")
        ejemplos = df[mask_final][['provincia', 'municipio', 'lat', 'lon']].head(10)
        for _, row in ejemplos.iterrows():
            print(f"  - {row['provincia']}, {row['municipio']}: ({row['lat']:.3f}, {row['lon']:.3f})")

    # Guardar
    os.makedirs(os.path.dirname(LOCAL_COORDS), exist_ok=True)
    df.to_csv(LOCAL_COORDS, index=False)
    print(f"Coordenadas guardadas: {df.shape[0]} municipios")
    return LOCAL_COORDS

def download_sube_csv(**kwargs):
    """Descarga los archivos SUBE para todos los años configurados."""
    urls = []

    for year in SUBE_YEARS:
        url = SUBE_URL_TEMPLATE.format(year=year)
        local_url = LOCAL_SUBE_TEMPLATE.format(year=year)
        os.makedirs(os.path.dirname(local_url), exist_ok=True)

        if os.path.exists(local_url) and os.path.getsize(local_url) > 0:
            urls.append(local_url)
            continue

        r = requests.get(url, stream=True, timeout=(10, 300))
        r.raise_for_status()
        with open(local_url, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK):
                if chunk:
                    f.write(chunk)
        urls.append(local_url)

    return urls

def download_feriados(**kwargs):
    paths = []
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

    for year in SUBE_YEARS:
        url = FERIADOS_URL_TEMPLATE.format(year=year)
        local_path = LOCAL_FERIADOS_TEMPLATE.format(year=year)

        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            paths.append(local_path)
            continue

        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(r.json(), f, indent=2, ensure_ascii=False)

        paths.append(local_path)

    return paths

def extract_feriados(**kwargs):
    ti = kwargs["ti"]
    source_paths = ti.xcom_pull(task_ids="download_feriados") or []

    if not source_paths:
        raise ValueError("No se encontraron archivos de feriados descargados")

    frames = []
    for path in source_paths:
        df = pd.read_json(path)
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["anio"] = df["fecha"].dt.year
        frames.append(df)

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.drop_duplicates(subset=["fecha", "nombre", "tipo"])
    df_all = df_all.sort_values("fecha")

    out = f"{OUTPUT_DIR}/feriados_extract.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_all.to_csv(out, index=False)
    return out

# Mejoras adicionales para el DAG

def normalize_text(texto):
    """Normaliza texto de manera consistente en todo el pipeline"""
    if texto is None or pd.isna(texto):
        return None
    
    texto = str(texto).strip()
    
    # Normaliza (NFKD = compatibilidad, descompone caracteres con tilde)
    nfkd = unicodedata.normalize("NFKD", texto)
    # Elimina marcas de acento
    normalized = "".join([c for c in nfkd if not unicodedata.combining(c)])
    
    # Limpieza adicional para casos problemáticos
    normalized = re.sub(r'\s+', ' ', normalized)  # múltiples espacios -> uno
    normalized = normalized.replace('  ', ' ')     # dobles espacios
    
    return normalized.lower()

def extract_sube(**kwargs):
    """Extracción de datos SUBE con normalización consistente"""
    ti = kwargs["ti"]
    source_paths = ti.xcom_pull(task_ids="download_sube_csv") or []

    if not source_paths:
        raise ValueError("No se encontraron archivos SUBE descargados")

    frames = []
    for path in source_paths:
        print(f"Leyendo SUBE: {os.path.basename(path)}")
        frames.append(pd.read_csv(path))

    df = pd.concat(frames, ignore_index=True)
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
    
    # IMPORTANTE: Normalizar los nombres AQUÍ para consistencia
    df["provincia"] = df["provincia"].apply(normalize_text)
    df["municipio"] = df["municipio"].apply(normalize_text)

    # Por las dudas, normalizamos el campo amba antes de filtrar
    df["amba"] = df["amba"].astype(str).str.strip().str.lower()
    
    # Filtramos para tener solamente los registros del AMBA
    df = df.loc[df["amba"].eq("si")].copy()

    # Eliminamos columnas innecesarias
    df = df.drop(columns=["dato_preliminar", "amba", "jurisdiccion"])
    
    # Debug: mostrar algunos municipios únicos para verificar normalización
    print("Municipios únicos en SUBE (primeros 10):")
    unique_munis = df[['provincia', 'municipio']].drop_duplicates().head(10)
    for _, row in unique_munis.iterrows():
        print(f"  - {row['provincia']}, {row['municipio']}")
    
    out = f"{OUTPUT_DIR}/sube_extract.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Datos SUBE procesados: {len(df)} registros, {df[['provincia', 'municipio']].drop_duplicates().shape[0]} municipios únicos")
    return out

def merge_coordinates(**kwargs):
    """Merge de coordenadas con datos SUBE"""
    ti = kwargs["ti"]
    sube_path = ti.xcom_pull(task_ids="extract_sube")
    
    # Leer datos SUBE completos (solo necesitamos municipios únicos)
    df_sube = pd.read_csv(sube_path)
    
    # Los datos SUBE ya están normalizados en extract_sube
    input_coord = df_sube[['provincia', 'municipio']].drop_duplicates().reset_index(drop=True)
    print(f"Municipios únicos en SUBE: {len(input_coord)}")
    
    # Leer coordenadas (ya están normalizadas)
    df_coords = pd.read_csv(LOCAL_COORDS)
    print(f"Municipios con coordenadas disponibles: {len(df_coords)}")
    
    # Merge directo (ambos datasets ya están normalizados)
    df_merged = pd.merge(
        left=input_coord,
        right=df_coords,
        on=['provincia', 'municipio'],  # Merge directo, sin normalización adicional
        how="left",
        indicator=True  # Para debug
    )
    
    # Debug: verificar matches
    matches = df_merged['_merge'].value_counts()
    print("Resultado del merge:")
    print(matches)
    
    if matches.get('left_only', 0) > 0:
        print(f"\nMunicipios sin coordenadas (primeros 10):")
        sin_coords = df_merged[df_merged['_merge'] == 'left_only'][['provincia', 'municipio']].head(10)
        for _, row in sin_coords.iterrows():
            print(f"  - {row['provincia']}, {row['municipio']}")
    
    # Eliminar la columna indicator
    df_merged = df_merged.drop('_merge', axis=1)
    
    # Verificar coordenadas válidas
    coords_validas = df_merged[df_merged['lat'].notna() & df_merged['lon'].notna()]
    print(f"Municipios con coordenadas válidas: {len(coords_validas)}")
    
    # Guardar TODOS los municipios (incluso sin coordenadas) para debug
    out_all = f"{OUTPUT_DIR}/municipios_merge_debug.csv"
    df_merged.to_csv(out_all, index=False)
    
    # Filtrar solo los que tienen coordenadas para el pipeline principal
    df_final = df_merged.dropna(subset=['lat', 'lon'])
    
    out = f"{OUTPUT_DIR}/municipios_with_coords.csv"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_final.to_csv(out, index=False)
    
    print(f"Resultado final: {len(df_final)} municipios con coordenadas de {len(input_coord)} municipios únicos en SUBE")
    print(f"Cobertura: {len(df_final)/len(input_coord)*100:.1f}%")
    
    return out

def enrich_with_weather(**kwargs):
    """Obtiene datos de clima con mejor manejo de errores y logging"""
    ti = kwargs["ti"]
    coords_path = ti.xcom_pull(task_ids="merge_coordinates")
    
    # Parámetros ajustados al rango de SUBE_YEARS
    min_year = min(SUBE_YEARS)
    max_year = max(SUBE_YEARS)
    fecha_inicio = f"{min_year}-01-01"
    fecha_fin = f"{max_year}-12-31"
    tz = "America/Argentina/Mendoza"
    
    # Leer municipios con coordenadas
    df_coords = pd.read_csv(coords_path)
    print(f"Obteniendo clima para {len(df_coords)} municipios entre {fecha_inicio} y {fecha_fin}")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    rows = []
    errores = 0
    
    for i, row in df_coords.iterrows():
        lat = row["lat"]
        lon = row["lon"]
        provincia = row["provincia"]
        municipio = row["municipio"]
        
        # Progress indicator cada 50 municipios
        if i % 50 == 0:
            print(f"Procesando municipio {i+1}/{len(df_coords)}: {provincia} - {municipio}")
        
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
                registros_municipio = 0
                for fecha, tmax, tmin, prec, viento in zip(
                    daily["time"],
                    daily.get("temperature_2m_max", []),
                    daily.get("temperature_2m_min", []),
                    daily.get("precipitation_sum", []),
                    daily.get("windspeed_10m_max", []),
                ):
                    rows.append({
                        "provincia": provincia,
                        "municipio": municipio,
                        "lat": float(lat),
                        "lon": float(lon),
                        "fecha": fecha,     
                        "tmax": tmax,
                        "tmin": tmin,
                        "precip": prec,
                        "viento": viento,
                    })
                    registros_municipio += 1
                
                if registros_municipio == 0:
                    print(f"Sin datos diarios para {provincia} - {municipio}")
       
        except Exception as e:
            errores += 1
            print(f"ERROR en {provincia} - {municipio}: {e}")
            # Para municipios con error, agregar al menos un registro con NaN
            # para mantener la referencia del municipio
            rows.append({
                "provincia": provincia,
                "municipio": municipio,
                "lat": float(lat),
                "lon": float(lon),
                "fecha": fecha_inicio,
                "tmax": None,
                "tmin": None,
                "precip": None,
                "viento": None,
            })
        
        # Pausa de cortesía
        time.sleep(0.05)
    
    df_clima = pd.DataFrame(rows)
    df_clima['fecha'] = pd.to_datetime(df_clima['fecha'])

    weather_suffix = str(min_year) if min_year == max_year else f"{min_year}_{max_year}"
    out = f"{OUTPUT_DIR}/weather_municipios_{weather_suffix}.csv"
    df_clima.to_csv(out, index=False)
    
    print(f"\n=== RESUMEN CLIMA ===")
    print(f"Municipios procesados: {len(df_coords)}")
    print(f"Registros de clima obtenidos: {len(df_clima)}")
    print(f"Errores: {errores}")
    print(f"Municipios únicos en datos clima: {df_clima[['provincia', 'municipio']].drop_duplicates().shape[0]}")
    
    return out

def merge_and_transform(**kwargs):
    """Merge final con mejor logging y diagnóstico"""
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

    # CREAR FUNCIÓN DE NORMALIZACIÓN EN DUCKDB (mejorada)
    con.execute("""
    CREATE OR REPLACE FUNCTION normalize_text(text_val) AS (
        CASE 
            WHEN text_val IS NULL THEN NULL
            ELSE LOWER(TRIM(REGEXP_REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    text_val, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ü', 'u'), 'ñ', 'n'), 'Ñ', 'n'), 'À', 'a'), 'È', 'e'),
                '\s+', ' ', 'g'
            )))
        END
    );
    """)

    # Estadísticas pre-merge
    print("\n=== ESTADÍSTICAS PRE-MERGE ===")
    
    sube_stats = con.execute(f"""
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT provincia || '|' || municipio) as municipios_unicos
    FROM read_csv_auto('{sube_path}')
    """).fetchall()
    print(f"SUBE: {sube_stats[0][0]} registros, {sube_stats[0][1]} municipios únicos")
    
    weather_stats = con.execute(f"""
    SELECT 
        COUNT(*) as total_registros,
        COUNT(DISTINCT provincia || '|' || municipio) as municipios_unicos
    FROM read_csv_auto('{weather_path}')
    """).fetchall()
    print(f"Weather: {weather_stats[0][0]} registros, {weather_stats[0][1]} municipios únicos")

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
            AND s.provincia = w.provincia
            AND s.municipio = w.municipio
        LEFT JOIN read_csv_auto('{feriados_path}', SAMPLE_SIZE=-1) AS f
            ON CAST(s.fecha AS DATE) = TRY_CAST(f.fecha AS DATE)
        )
        TO '{out}' (HEADER, DELIMITER ',');
    """)

    # Estadísticas post-merge
    print("\n=== ESTADÍSTICAS POST-MERGE ===")
    
    final_stats = con.execute(f"""
    SELECT 
        COUNT(*) as total_registros,
        COUNT(CASE WHEN tmax IS NOT NULL THEN 1 END) as registros_con_clima,
        COUNT(CASE WHEN is_feriado = 1 THEN 1 END) as registros_feriados,
        COUNT(DISTINCT provincia || '|' || municipio) as municipios_unicos
    FROM read_csv_auto('{out}')
    """).fetchall()
    
    total, con_clima, feriados, municipios = final_stats[0]
    print(f"Archivo final: {total} registros")
    print(f"Con datos de clima: {con_clima} ({con_clima/total*100:.1f}%)")
    print(f"En feriados: {feriados} ({feriados/total*100:.1f}%)")
    print(f"Municipios únicos: {municipios}")

    con.close()
    print(f"\nArchivo final creado: {out}")
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

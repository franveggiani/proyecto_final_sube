"""
Pipeline SUBE + Feriados + Clima (versión explicada para Google Colab)

Objetivo
--------
Construir un dataset final que una:
- Usos de SUBE (viajes diarios por provincia/municipio)
- Feriados nacionales
- Clima histórico diario por municipio (tmax, tmin, precipitación y viento)

Tecnologías
-----------
- requests: para consumir APIs públicas (SUBE, feriados, georef, Open-Meteo)
- pandas: para preprocesar/filtros y manejo tabular simple
- duckdb: para hacer los joins pesados directamente sobre archivos CSV, evitando
  cargar todo a memoria (ideal cuando el dataset es grande y estamos en Colab).

Por qué DuckDB
--------------
DuckDB permite:
- Leer CSV masivos con read_csv_auto sin cargarlos en un DataFrame gigante.
- Ejecutar joins y transformaciones de manera vectorizada y paralela
  (configurable con PRAGMA threads) dentro del mismo proceso de Python.
- Exportar resultados a CSV con COPY de forma eficiente.

Estrategia general
------------------
1) Descarga de datos: SUBE (CSV grande) y feriados (JSON)
2) Descarga de coordenadas de todos los municipios (API georef)
3) Limpieza/normalización de texto para evitar fallos de merge por acentos y espacios
4) Enriquecimiento con clima histórico (Open-Meteo) para cada municipio
5) Join final en DuckDB por fecha y por provincia/municipio normalizados
6) Exportación a CSV del dataset final

Notas prácticas para Colab
--------------------------
- Este script usa rutas relativas bajo ./output para que sea portable.
- Puedes ejecutar sección por sección (las funciones) o correr main().
- Asegúrate de instalar dependencias en Colab: !pip install pandas requests duckdb
"""

from __future__ import annotations

import os
import time
import json
from typing import Optional, Tuple

import pandas as pd
import requests


# -----------------------
# Configuración de rutas y fuentes
# -----------------------

# Fuente de SUBE 2024 (Ministerio de Transporte)
SUBE_URL = "https://archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos/dat-ab-usos-2024.csv"

# Directorios locales (ajustados para Colab/entorno local)
DATA_DIR = "./data"
OUTPUT_DIR = "./output"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOCAL_SUBE = os.path.join(DATA_DIR, "dat-ab-usos-2024.csv")
LOCAL_FERIADOS = os.path.join(DATA_DIR, "feriados_2024.json")
LOCAL_COORDS = os.path.join(DATA_DIR, "municipios_coords.csv")

# Config de descarga en streaming para evitar picos de memoria
CHUNK = 1024 * 1024


# -----------------------
# Utilidades de normalización
# -----------------------

def normalize_text(text_value: Optional[str]) -> Optional[str]:
    """Normaliza texto: trim, lower y reemplazo básico de acentos/especiales.

    Esta función se usa en Python y su lógica también se replica en DuckDB
    (con una UDF SQL) para asegurar que las claves de join (provincia/municipio)
    se comparen de forma robusta.
    """
    if text_value is None or pd.isna(text_value):
        return None
    s = str(text_value).strip().lower()
    replacements = (
        ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
        ("ü", "u"), ("ñ", "n"), ("Ñ", "n"),
    )
    for a, b in replacements:
        s = s.replace(a, b)
    return s


# -----------------------
# Paso 1: Descarga de datos
# -----------------------

def download_sube_csv() -> str:
    """Descarga el CSV de SUBE 2024 en streaming para no usar mucha memoria.

    Retorna la ruta local del archivo descargado.
    """
    if os.path.exists(LOCAL_SUBE):
        print(f"SUBE ya está descargado en: {LOCAL_SUBE}")
        return LOCAL_SUBE

    print("Descargando SUBE 2024 (esto puede tardar)...")
    r = requests.get(SUBE_URL, stream=True, timeout=(10, 300))
    r.raise_for_status()
    with open(LOCAL_SUBE, "wb") as f:
        for chunk in r.iter_content(chunk_size=CHUNK):
            if chunk:
                f.write(chunk)
    print(f"SUBE descargado en: {LOCAL_SUBE}")
    return LOCAL_SUBE


def download_feriados() -> str:
    """Descarga los feriados 2024 (JSON) y guarda en disco."""
    url = "https://api.argentinadatos.com/v1/feriados/2024"
    if os.path.exists(LOCAL_FERIADOS):
        print(f"Feriados ya existen en: {LOCAL_FERIADOS}")
        return LOCAL_FERIADOS

    print("Descargando feriados 2024...")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with open(LOCAL_FERIADOS, "w", encoding="utf-8") as f:
        json.dump(r.json(), f, indent=2, ensure_ascii=False)
    print(f"Feriados guardados en: {LOCAL_FERIADOS}")
    return LOCAL_FERIADOS


def fetch_all_municipios() -> str:
    """Obtiene coordenadas de todos los municipios (API georef) y guarda CSV.

    - Paginación con parámetros inicio/max
    - Columnas de salida: provincia, municipio, lat, lon
    """
    url = "https://apis.datos.gob.ar/georef/api/municipios"
    params = {"aplanar": "true", "max": 5000, "inicio": 0}
    frames = []
    total = None
    print("Descargando municipios + centroides...")
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
        time.sleep(0.15)  # cortesía para no saturar la API

    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={
        "nombre": "municipio",
        "provincia_nombre": "provincia",
        "centroide_lat": "lat",
        "centroide_lon": "lon",
    })[["provincia", "municipio", "lat", "lon"]]

    # Nota: guardamos sin normalizar; normalizaremos en los merges
    df.to_csv(LOCAL_COORDS, index=False)
    print(f"Coordenadas guardadas: {df.shape[0]} municipios -> {LOCAL_COORDS}")
    return LOCAL_COORDS


# -----------------------
# Paso 2: Extract/limpieza básica
# -----------------------

def extract_sube() -> str:
    """Lee SUBE 2024, renombra columnas clave y elimina las no usadas.

    Exporta un CSV limpio para downstream.
    """
    df = pd.read_csv(LOCAL_SUBE)
    df["DIA_TRANSPORTE"] = pd.to_datetime(df["DIA_TRANSPORTE"], errors="coerce")
    df = df.rename(
        columns={
            "DIA_TRANSPORTE": "fecha",
            "PROVINCIA": "provincia",
            "MUNICIPIO": "municipio",
            "NOMBRE_EMPRESA": "empresa",
            "LINEA": "linea",
            "AMBA": "amba",
            "TIPO_TRANSPORTE": "tipo_transporte",
            "JURISDICCION": "jurisdiccion",
            "CANTIDAD": "cantidad",
            "DATO_PRELIMINAR": "dato_preliminar",
        }
    )
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    # Eliminamos columnas que no se usan en análisis final
    df = df.drop(columns=["dato_preliminar", "amba", "jurisdiccion"], errors="ignore")

    out = os.path.join(OUTPUT_DIR, "sube_extract.csv")
    df.to_csv(out, index=False)
    print(f"SUBE limpio -> {out}")
    return out


def extract_feriados() -> str:
    """Convierte el JSON de feriados a CSV simple para facilitar el join."""
    df = pd.read_json(LOCAL_FERIADOS)
    # Conservamos la columna 'fecha' (ya viene así) y resto de metadata
    out = os.path.join(OUTPUT_DIR, "feriados_extract.csv")
    df.to_csv(out, index=False)
    print(f"Feriados CSV -> {out}")
    return out


def merge_coordinates(sube_path: str) -> str:
    """Cruza municipios únicos de SUBE con coordenadas descargadas.

    Normaliza nombres en ambos lados para reducir mismatches por acentos/espacios.
    """
    df_sube = pd.read_csv(sube_path)
    unique_munis = df_sube[["provincia", "municipio"]].drop_duplicates().reset_index(drop=True)

    df_coords = pd.read_csv(LOCAL_COORDS)
    # Normalizamos ambos lados a claves auxiliares
    unique_munis["prov_key"] = unique_munis["provincia"].apply(normalize_text)
    unique_munis["muni_key"] = unique_munis["municipio"].apply(normalize_text)

    df_coords["prov_key"] = df_coords["provincia"].apply(normalize_text)
    df_coords["muni_key"] = df_coords["municipio"].apply(normalize_text)

    merged = pd.merge(
        left=unique_munis,
        right=df_coords,
        how="left",
        left_on=["prov_key", "muni_key"],
        right_on=["prov_key", "muni_key"],
    )[["provincia_x", "municipio_x", "lat", "lon"]].rename(
        columns={"provincia_x": "provincia", "municipio_x": "municipio"}
    )

    merged = merged.dropna(subset=["lat", "lon"])  # descartamos sin coordenadas
    out = os.path.join(OUTPUT_DIR, "municipios_with_coords.csv")
    merged.to_csv(out, index=False)
    print(f"Municipios con coords -> {out} ({merged.shape[0]} filas)")
    return out


# -----------------------
# Paso 3: Enriquecimiento con clima (Open-Meteo)
# -----------------------

def enrich_with_weather(coords_path: str,
                        fecha_inicio: str = "2024-01-01",
                        fecha_fin: str = "2024-12-31",
                        tz: str = "America/Argentina/Mendoza") -> str:
    """Obtiene clima histórico diario para cada municipio con coordenadas.

    Advertencia: Puede tardar (muchos municipios x 365 días). Pensado para ejecutar
    con paciencia en Colab. Se añade una pequeña pausa por cortesía a la API.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    df_coords = pd.read_csv(coords_path)

    rows = []
    print("Consultando Open-Meteo por municipio (histórico diario)...")
    for i, row in df_coords.iterrows():
        lat = float(row["lat"]) 
        lon = float(row["lon"]) 
        provincia = row["provincia"]
        municipio = row["municipio"]

        try:
            resp = requests.get(
                url,
                params={
                    "latitude": lat,
                    "longitude": lon,
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
                        "provincia": provincia,
                        "municipio": municipio,
                        "lat": lat,
                        "lon": lon,
                        "fecha": fecha,
                        "tmax": tmax,
                        "tmin": tmin,
                        "precip": prec,
                        "viento": viento,
                    })
        except Exception as e:
            print(f"ERROR en fila {i} ({provincia} - {municipio}): {e}")

        time.sleep(0.05)  # cortesía

    df_weather = pd.DataFrame(rows)
    df_weather["fecha"] = pd.to_datetime(df_weather["fecha"], errors="coerce")
    out = os.path.join(OUTPUT_DIR, f"weather_municipios_{fecha_fin[:4]}.csv")
    df_weather.to_csv(out, index=False)
    print(f"Clima generado -> {out} ({len(rows)} filas)")
    return out


# -----------------------
# Paso 4: Join final con DuckDB
# -----------------------

def merge_and_transform(sube_path: str, feriados_path: str, weather_path: str, date_tag: str) -> str:
    """Une SUBE + clima + feriados usando DuckDB, con normalización en SQL."""
    import duckdb

    out = os.path.join(OUTPUT_DIR, f"final_{date_tag}.csv")
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")

    # UDF de normalización equivalente a la de Python
    con.execute(
        """
        CREATE OR REPLACE FUNCTION normalize_text(text_val) AS (
            CASE 
                WHEN text_val IS NULL THEN NULL
                ELSE LOWER(TRIM(
                    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                        text_val, 'á', 'a'), 'é', 'e'), 'í', 'i'), 'ó', 'o'), 'ú', 'u'), 'ü', 'u'), 'ñ', 'n'), 'Ñ', 'n')
                ))
            END
        );
        """
    )

    # Join directo sobre CSVs, exportando a CSV
    con.execute(
        f"""
        COPY (
            SELECT
                s.*,
                w.tmax, w.tmin, w.precip, w.viento,
                'Grupo 17' AS grupo
            FROM read_csv_auto('{sube_path}', SAMPLE_SIZE=-1) AS s
            LEFT JOIN read_csv_auto('{weather_path}', SAMPLE_SIZE=-1) AS w
                ON CAST(s.fecha AS DATE) = TRY_CAST(w.fecha AS DATE)
                AND normalize_text(s.provincia) = normalize_text(w.provincia)
                AND normalize_text(s.municipio) = normalize_text(w.municipio)
            LEFT JOIN read_csv_auto('{feriados_path}', SAMPLE_SIZE=-1) AS f
                ON CAST(s.fecha AS DATE) = TRY_CAST(f.fecha AS DATE)
        )
        TO '{out}' (HEADER, DELIMITER ',');
        """
    )

    # Métricas de verificación
    matches = con.execute(
        f"""
        SELECT COUNT(*) as matches
        FROM read_csv_auto('{sube_path}') s
        JOIN read_csv_auto('{weather_path}') w
            ON CAST(s.fecha AS DATE) = TRY_CAST(w.fecha AS DATE)
            AND normalize_text(s.provincia) = normalize_text(w.provincia)
            AND normalize_text(s.municipio) = normalize_text(w.municipio)
        """
    ).fetchall()
    print(f"Matches SUBE<->clima: {matches}")

    # Muestras para entender las claves
    sample_weather = con.execute(
        f"SELECT DISTINCT provincia, municipio FROM read_csv_auto('{weather_path}') LIMIT 10"
    ).df()
    print("Sample weather:")
    print(sample_weather)

    sample_sube = con.execute(
        f"SELECT DISTINCT provincia, municipio FROM read_csv_auto('{sube_path}') LIMIT 10"
    ).df()
    print("Sample SUBE:")
    print(sample_sube)

    print(f"Archivo final creado -> {out}")
    return out


# -----------------------
# Ejecución secuencial (para Colab)
# -----------------------

def main():
    """Corre todo el pipeline de punta a punta en Colab/entorno local."""
    # 1) Descargas
    sube_csv = download_sube_csv()
    feriados_json = download_feriados()
    coords_csv = fetch_all_municipios()

    # 2) Extract/limpieza
    sube_extract = extract_sube()
    feriados_extract = extract_feriados()
    municipios_with_coords = merge_coordinates(sube_extract)

    # 3) Enriquecer con clima (rango anual). Puedes reducir el rango si necesitas acelerar.
    weather_csv = enrich_with_weather(
        municipios_with_coords,
        fecha_inicio="2024-01-01",
        fecha_fin="2024-12-31",
        tz="America/Argentina/Mendoza",
    )

    # 4) Join final en DuckDB
    final_csv = merge_and_transform(
        sube_path=sube_extract,
        feriados_path=feriados_extract,
        weather_path=weather_csv,
        date_tag="colab",
    )

    print("\n=== RESULTADOS ===")
    print(f"SUBE limpio: {sube_extract}")
    print(f"Feriados CSV: {feriados_extract}")
    print(f"Municipios+coords: {municipios_with_coords}")
    print(f"Clima CSV: {weather_csv}")
    print(f"Final CSV: {final_csv}")


if __name__ == "__main__":
    # En Colab, puedes ejecutar main() o correr función por función.
    # Recomendación: si el clima de todo el año tarda demasiado, prueba un rango más corto
    # en enrich_with_weather(fecha_inicio, fecha_fin) para validar el pipeline.
    main()



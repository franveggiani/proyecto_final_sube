## Proyecto: Análisis y prediccion de demanda de transporte público con datos SUBE

### Descripción del problema a abordar

Analizar cómo varían los usos del transporte (SUBE) en Argentina a lo largo del tiempo y entre regiones, incorporando el **contexto de clima** y **feriados** para explicar picos, caídas y patrones estacionales en la demanda.


**Grupo 17**. Este repositorio contiene un DAG de Airflow que construye de forma reproducible un dataset final uniendo:
- **Usos SUBE 2024** (viajes diarios por provincia/municipio)
- **Feriados 2024** (API Argentina Datos)
- **Clima histórico diario** por municipio (Open‑Meteo) enriquecido a partir de coordenadas de la API Georef (Datos Argentina)

El pipeline está diseñado para ejecutarse en un entorno controlado con Astro CLI (Docker) y generar automáticamente los archivos de salida necesarios. Para el join final se utiliza **DuckDB**, lo que evita saturar de memoria a los workers de Airflow al operar directamente sobre archivos CSV.

### Consigna: Primera Entrega – Ingeniería de Datos (10/09/2025)

- **Entregable**: Un DAG funcional de Airflow que ejecute el proceso completo de obtención, carga y preparación inicial del dataset, capaz de generar el archivo de datos final de manera reproducible.
- **Demostración**: Se mostrará en vivo la ejecución del DAG. Si la ejecución completa insume demasiado tiempo, se presentarán **logs de una ejecución previa** que reflejen todas las tareas correctamente ejecutadas.
- **Documentación**: Incluimos aquí una breve descripción del problema, fuentes de datos y decisiones de diseño del pipeline.



### Fuentes de datos

- **SUBE 2024 (CSV)**: Ministerio de Transporte.
  - URL usada en `dags/final_dag.py`: `https://archivos-datos.transporte.gob.ar/upload/Dat_Ab_Usos/dat-ab-usos-2024.csv`
- **Feriados 2024 (JSON)**: `https://api.argentinadatos.com/v1/feriados/2024`
- **Georef Municipios (API)**: `https://apis.datos.gob.ar/georef/api/municipios` (coordenadas por municipio)
- **Clima histórico (API)**: `https://archive-api.open-meteo.com/v1/archive`

### Decisiones de diseño del pipeline

- **Airflow**: Orquestación del ETL con `PythonOperator` y dependencias explícitas. Se usan XComs para pasar rutas de archivos entre tareas.
- **Pandas + Requests**: Extracciones y preprocesos ligeros de CSV/JSON y consumo de APIs.
- **DuckDB para joins pesados**: El join de SUBE (voluminoso) con clima (gran cardinalidad) y feriados se realiza en SQL sobre CSV con DuckDB:
  - Lectura directa con `read_csv_auto` sin crear DataFrames gigantes.
  - Ejecución vectorizada, paralelismo (`PRAGMA threads`), y exportación a CSV vía `COPY`.
  - UDF `normalize_text` en SQL para robustecer los joins por `provincia/municipio` (manejo de tildes/espacios/minúsculas).
- **Reproducibilidad**: Docker (Astro Runtime) + `requirements.txt` (incluye `duckdb`, `pandas`, `requests`).
- **Logs**: Se empaquetan en una tarea final para facilitar la evidencia de ejecución si el proceso completo demora.

### DAGs principales

- `dags/final_dag.py` (`dag_id=tp1_grupo17`): pipeline completo anual (SUBE + coords + clima anual + feriados + join en DuckDB + zip de logs).
- `dags/simple_dag.py` (`dag_id=simple_tp1_grupo17`): variante “rápida” para un único día; útil para demostración breve.

Visual del grafo del DAG principal:

![tp1_grupo17 graph](public/tp1_grupo17-graph%20(1).png)

### Pasos del DAG principal (`tp1_grupo17`)

- `download_sube_csv`: Descarga SUBE 2024 en streaming (chunked).
- `download_feriados`: Descarga feriados 2024 (JSON).
- `fetch_all_municipios`: Descarga todos los municipios con centroides (lat/lon).
- `extract_sube`: Limpieza/renombrado de SUBE y exportación a CSV.
- `extract_feriados`: Conversión de JSON de feriados a CSV.
- `merge_coordinates`: Merge de municipios de SUBE con coordenadas (normalización de texto).
- `enrich_with_weather`: Consulta Open‑Meteo por municipio para el rango anual (tmax/tmin/precip/viento por día).
- `merge_and_transform`: Join final en DuckDB entre SUBE + clima + feriados; exporta `final_{ds}.csv`.
- `export_logs_zip`: Comprime los logs de la ejecución.



### Evidencia y logs de una ejecución previa

Si el pipeline completo demora (por la descarga de clima anual para todos los municipios), se aceptará mostrar:
- El archivo final generado anteriormente en `local_output/output/final_YYYY-MM-DD.csv` (si existe en el repo o tras una corrida), y
- El zip de logs generado por la tarea `export_logs_zip`, por ejemplo `local_output/output/logs_YYYY-MM-DD.zip`.

También el proyecto produce salidas intermedias (SUBE limpio, feriados, municipios con coordenadas, clima) bajo `output/` dentro del contenedor. En este repo se incluyen ejemplos bajo `local_output/final/` correspondientes a la última corrida del DAG principal (fecha actual), incluyendo:

- `local_output/final/final_YYYY-MM-DD.csv` (dataset final)
- `local_output/final/logs_YYYY-MM-DD.zip` (logs comprimidos)
- `local_output/final/sube_extract.csv`, `local_output/final/feriados_extract.csv`, `local_output/final/municipios_with_coords.csv`, `local_output/final/weather_municipios_2024.csv` (artefactos intermedios)

### Vista previa del dataset final (head)

Tabla con las primeras filas de `local_output/final/final_2025-09-10.csv`:

| fecha | empresa | linea | tipo_transporte | provincia | municipio | cantidad | tmax | tmin | precip | viento | grupo |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---|
| 2024-05-15 | EMPRESA RECREO S R L | LINEA_018_SFE | COLECTIVO | SANTA FE | SANTA FE | 9830 | 15.4 | 5.5 | 0.0 | 10.4 | Grupo 17 |
| 2024-05-15 | SANTA ANA SRL | LINEA_019_JUJ | COLECTIVO | JUJUY | SAN SALVADOR DE JUJUY | 2045 | 12.8 | 1.5 | 0.0 | 31.0 | Grupo 17 |
| 2024-05-15 | EMPRESA DE TRANSPORTE AMANCAY SRL | LINEA_020_BRC | COLECTIVO | RÍO NEGRO | SAN CARLOS DE BARILOCHE | 5721 | 7.8 | 0.8 | 0.0 | 15.1 | Grupo 17 |
| 2024-05-15 | SANTA ANA SRL | LINEA_020_JUJ | COLECTIVO | JUJUY | SAN SALVADOR DE JUJUY | 5416 | 12.8 | 1.5 | 0.0 | 31.0 | Grupo 17 |
| 2024-05-15 | ERSA URBANO SA | LINEA_020_PRN | COLECTIVO | ENTRE RÍOS | PARANA | 2969 | 14.9 | 3.4 | 0.0 | 9.8 | Grupo 17 |


### Grupo 17 - Ciencia de Datos
#### Integrantes: Franco Veggiani, Juan Ignacio Diaz

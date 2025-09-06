import requests
import pandas as pd

def coords_por_municipio(provincia: str, municipio: str):
    r = requests.get(
        "https://apis.datos.gob.ar/georef/api/municipios",
        params={
            "nombre": municipio,
            "provincia": provincia,
            "aplanar": "true",
            "max": 1
        },
        timeout=20
    )
    r.raise_for_status()
    data = r.json()

    if not data.get("municipios"):
        raise ValueError(f"No se encontró municipio '{municipio}' en provincia '{provincia}'")
    
    m = data["municipios"][0]
    return float(m["centroide_lat"]), float(m["centroide_lon"]), m["id"], m["nombre"]

def clima_diario_por_municipio(provincia: str, municipio: str, fecha: str, tz: str = "America/Argentina/Mendoza"):
    
    lat, lon, muni_id, muni_nombre = coords_por_municipio(provincia, municipio)

    r = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude": lat,
            "longitude": lon,
            "start_date": fecha,      # yyyy-mm-dd
            "end_date": fecha,        # igual al start para “ese día”
            "daily": "temperature_2m_max,temperature_2m_min",
            "timezone": tz,
        },
        timeout=40
    )

    r.raise_for_status()
    d = r.json().get("daily", {})
    
    if not d:
        raise RuntimeError("Sin datos diarios devueltos")
    
    df = pd.DataFrame(d)
    df["time"] = pd.to_datetime(df["time"])
    
    # Devolver una fila (ese día) + metadata
    row = df.iloc[0].to_dict()
    
    return {
        "provincia": provincia,
        "municipio": muni_nombre,
        "municipio_id": muni_id,
        "fecha": str(row["time"].date()),
        "tmax": float(row["temperature_2m_max"]),
        "tmin": float(row["temperature_2m_min"]),
        "lat": lat,
        "lon": lon,
        "timezone": tz,
    }

# Ejemplo
res = clima_diario_por_municipio("Mendoza", "Godoy Cruz", "2025-08-15")
print(res)


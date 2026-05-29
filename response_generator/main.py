from fastapi import FastAPI
import pandas as pd

app = FastAPI()

# ==========================================
# 1. EL MAPA: Bounding boxes de las 5 zonas
# ==========================================
ZONAS = {
    "Z1": {"lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600}, # Providencia
    "Z2": {"lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550}, # Las Condes
    "Z3": {"lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740}, # Maipú
    "Z4": {"lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630}, # Santiago Centro
    "Z5": {"lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760}  # Pudahuel
}

# Variable global para guardar nuestro dataset
df_edificios = None

# ==========================================
# 2. EL CONOCIMIENTO: Cargar datos al iniciar
# ==========================================
@app.on_event("startup")
def cargar_datos():
    global df_edificios
    print("Cargando dataset en memoria...")
    
    # Dataset sintético de prueba
    datos_prueba = {
        "latitude": [-33.435, -33.410, -33.510, -33.450],
        "longitude": [-70.620, -70.580, -70.760, -70.650],
        "area_in_meters": [120.5, 85.0, 200.0, 150.0],
        "confidence": [0.9, 0.6, 0.85, 0.4]
    }
    df_edificios = pd.DataFrame(datos_prueba)
    print("¡Datos cargados y listos!")

# Función auxiliar para evitar caídas
def validar_estado():
    return df_edificios is not None

# ==========================================
# 3. LAS CONSULTAS MATEMÁTICAS (Q1 - Q5)
# ==========================================

@app.get("/q1")
def q1_conteo_edificios(zona_id: str, confidence_min: float = 0.0):
    if not validar_estado(): return {"error": "Dataset no cargado aún"}
    if zona_id not in ZONAS: return {"error": "Zona no válida. Use Z1 a Z5."}
        
    limites = ZONAS[zona_id]
    filtro_lat = (df_edificios['latitude'] >= limites['lat_min']) & (df_edificios['latitude'] <= limites['lat_max'])
    filtro_lon = (df_edificios['longitude'] >= limites['lon_min']) & (df_edificios['longitude'] <= limites['lon_max'])
    filtro_conf = df_edificios['confidence'] >= confidence_min
    
    resultado = df_edificios[filtro_lat & filtro_lon & filtro_conf]
    
    return {
        "consulta": "Q1",
        "zona": zona_id,
        "confidence_min": confidence_min,
        "conteo_edificios": len(resultado)
    }

@app.get("/q2")
def q2_area_total(zona_id: str, confidence_min: float = 0.0):
    if not validar_estado(): return {"error": "Dataset no cargado aún"}
    if zona_id not in ZONAS: return {"error": "Zona no válida. Use Z1 a Z5."}
        
    limites = ZONAS[zona_id]
    filtro_lat = (df_edificios['latitude'] >= limites['lat_min']) & (df_edificios['latitude'] <= limites['lat_max'])
    filtro_lon = (df_edificios['longitude'] >= limites['lon_min']) & (df_edificios['longitude'] <= limites['lon_max'])
    filtro_conf = df_edificios['confidence'] >= confidence_min
    
    resultado = df_edificios[filtro_lat & filtro_lon & filtro_conf]
    
    return {
        "consulta": "Q2",
        "zona": zona_id,
        "confidence_min": confidence_min,
        "area_total_metros": float(resultado['area_in_meters'].sum()) 
    }

@app.get("/q3")
def q3_area_promedio(zona_id: str, confidence_min: float = 0.0):
    if not validar_estado(): return {"error": "Dataset no cargado aún"}
    if zona_id not in ZONAS: return {"error": "Zona no válida. Use Z1 a Z5."}
        
    limites = ZONAS[zona_id]
    filtro_lat = (df_edificios['latitude'] >= limites['lat_min']) & (df_edificios['latitude'] <= limites['lat_max'])
    filtro_lon = (df_edificios['longitude'] >= limites['lon_min']) & (df_edificios['longitude'] <= limites['lon_max'])
    filtro_conf = df_edificios['confidence'] >= confidence_min
    
    resultado = df_edificios[filtro_lat & filtro_lon & filtro_conf]
    area_promedio = 0.0 if resultado.empty else resultado['area_in_meters'].mean()
    
    return {
        "consulta": "Q3",
        "zona": zona_id,
        "confidence_min": confidence_min,
        "area_promedio_metros": float(area_promedio)
    }

@app.get("/q4")
def q4_area_maxima(zona_id: str, confidence_min: float = 0.0):
    if not validar_estado(): return {"error": "Dataset no cargado aún"}
    if zona_id not in ZONAS: return {"error": "Zona no válida. Use Z1 a Z5."}
        
    limites = ZONAS[zona_id]
    filtro_lat = (df_edificios['latitude'] >= limites['lat_min']) & (df_edificios['latitude'] <= limites['lat_max'])
    filtro_lon = (df_edificios['longitude'] >= limites['lon_min']) & (df_edificios['longitude'] <= limites['lon_max'])
    filtro_conf = df_edificios['confidence'] >= confidence_min
    
    resultado = df_edificios[filtro_lat & filtro_lon & filtro_conf]
    area_max = 0.0 if resultado.empty else resultado['area_in_meters'].max()
    
    return {
        "consulta": "Q4",
        "zona": zona_id,
        "confidence_min": confidence_min,
        "area_maxima_metros": float(area_max)
    }

@app.get("/q5")
def q5_area_minima(zona_id: str, confidence_min: float = 0.0):
    if not validar_estado(): return {"error": "Dataset no cargado aún"}
    if zona_id not in ZONAS: return {"error": "Zona no válida. Use Z1 a Z5."}
        
    limites = ZONAS[zona_id]
    filtro_lat = (df_edificios['latitude'] >= limites['lat_min']) & (df_edificios['latitude'] <= limites['lat_max'])
    filtro_lon = (df_edificios['longitude'] >= limites['lon_min']) & (df_edificios['longitude'] <= limites['lon_max'])
    filtro_conf = df_edificios['confidence'] >= confidence_min
    
    resultado = df_edificios[filtro_lat & filtro_lon & filtro_conf]
    area_min = 0.0 if resultado.empty else resultado['area_in_meters'].min()
    
    return {
        "consulta": "Q5",
        "zona": zona_id,
        "confidence_min": confidence_min,
        "area_minima_metros": float(area_min)
    }
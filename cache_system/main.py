from fastapi import FastAPI
import redis
import requests
import json

app = FastAPI()

# Conexión a nuestra base de datos Redis en la red de Docker
cache = redis.Redis(host='redis_db', port=6379, db=0, decode_responses=True)

# URL interna donde vive nuestro Cerebro (Generador de Respuestas)
CEREBRO_URL = "http://response_generator:8001"

# Tiempo de vida de los datos en caché (Time To Live). Lo pondremos en 60 segundos por ahora.
TTL_SECONDS = 60

def procesar_consulta(cache_key: str, endpoint: str, params: dict):
    """
    Esta función implementa el flujo exacto de la Figura 1 del informe.
    """
    # 1. ¿Existe en caché? (Cache Hit)
    cached_data = cache.get(cache_key)
    if cached_data:
        respuesta = json.loads(cached_data)
        respuesta["origen"] = "Cache Hit (Redis)"
        return respuesta
        
    # 2. Si no existe (Cache Miss), delegamos la tarea al Cerebro
    respuesta_cerebro = requests.get(f"{CEREBRO_URL}{endpoint}", params=params).json()
    
    # Si el cerebro nos devuelve un error (ej: zona inválida), no lo guardamos en caché
    if "error" in respuesta_cerebro:
        return respuesta_cerebro
        
    # 3. Almacenamos el resultado en caché con su TTL
    cache.setex(cache_key, TTL_SECONDS, json.dumps(respuesta_cerebro))
    
    # Le agregamos una etiqueta para que sepas de dónde vino
    respuesta_cerebro["origen"] = "Cache Miss (Calculado por el Cerebro)"
    return respuesta_cerebro

# ==========================================
# ENDPOINTS DEL CACHÉ (Interceptan el tráfico)
# ==========================================

@app.get("/q1")
def q1(zona_id: str, confidence_min: float = 0.0):
    cache_key = f"count:{zona_id}:conf={confidence_min}"
    return procesar_consulta(cache_key, "/q1", {"zona_id": zona_id, "confidence_min": confidence_min})

@app.get("/q2")
def q2(zona_id: str, confidence_min: float = 0.0):
    cache_key = f"area:{zona_id}:conf={confidence_min}"
    return procesar_consulta(cache_key, "/q2", {"zona_id": zona_id, "confidence_min": confidence_min})

@app.get("/q3")
def q3(zona_id: str, confidence_min: float = 0.0):
    cache_key = f"density:{zona_id}:conf={confidence_min}"
    return procesar_consulta(cache_key, "/q3", {"zona_id": zona_id, "confidence_min": confidence_min})

@app.get("/q4")
def q4(zona_a: str, zona_b: str, confidence_min: float = 0.0):
    cache_key = f"compare:density:{zona_a}:{zona_b}:conf={confidence_min}"
    return procesar_consulta(cache_key, "/q4", {"zona_a": zona_a, "zona_b": zona_b, "confidence_min": confidence_min})

@app.get("/q5")
def q5(zona_id: str, bins: int = 5):
    cache_key = f"confidence_dist:{zona_id}:bins={bins}"
    return procesar_consulta(cache_key, "/q5", {"zona_id": zona_id, "bins": bins})

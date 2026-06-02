import time
import json
import requests
import csv
import os
from confluent_kafka import Consumer, Producer

# ==========================================
# CONFIGURACIONES
# ==========================================
KAFKA_BROKER = "kafka:9092"
TOPIC_PRINCIPAL = "consultas_topic"
TOPIC_REINTENTOS = "reintentos_topic" 
TOPIC_DLQ = "dlq_topic" 
CACHE_URL = "http://cache_system:8000"
MAX_REINTENTOS = 3

# ¡LA MEMORIA INTELIGENTE!
archivos_limpiados = set()

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ [ERROR KAFKA] No se pudo enviar el mensaje: {err}')

def guardar_metrica(id_consulta, t_creacion, t_final, origen, distribucion):
    global archivos_limpiados
    latencia_total = t_final - t_creacion
    archivo_actual = f"/app/metricas_{distribucion}.csv" 
    
    # Si es el PRIMER mensaje de esta distribución que vemos en esta ejecución, SOBREESCRIBIMOS el archivo
    if distribucion not in archivos_limpiados:
        if os.path.exists(archivo_actual):
            os.remove(archivo_actual) # Borramos el archivo viejo
        archivos_limpiados.add(distribucion) # Anotamos que ya lo limpiamos
        print(f"🧹 Archivo {archivo_actual} reescrito desde cero para la nueva prueba de {distribucion}.")

    es_nuevo = not os.path.exists(archivo_actual)
    
    with open(archivo_actual, mode='a', newline='') as file:
        writer = csv.writer(file)
        if es_nuevo:
            writer.writerow(['id_consulta', 'distribucion', 'origen', 'tiempo_creacion', 'tiempo_procesado', 'latencia_segundos'])
        writer.writerow([id_consulta, distribucion, origen, t_creacion, t_final, round(latencia_total, 4)])

def iniciar_consumidor():
    print("Preparando Consumidor... esperando a que Kafka despierte.")
    time.sleep(10)
    
    # 1. Configuración del Consumidor
    conf_consumidor = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'grupo_procesamiento_1',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf_consumidor)
    consumer.subscribe([TOPIC_PRINCIPAL, TOPIC_REINTENTOS])
    
    # 2. Configuración del Productor (Para Reintentos y DLQ)
    conf_productor = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(conf_productor)

    print(f"¡Suscrito a {TOPIC_PRINCIPAL} y {TOPIC_REINTENTOS}! Escuchando...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error(): continue
            
            # --- DESEMPAQUETAR MENSAJE ---
            datos = json.loads(msg.value().decode('utf-8'))
            consulta_id = datos.get("id_consulta")
            tipo_consulta = datos.get("consulta")
            parametros = datos.get("parametros")
            t_creacion = datos.get("timestamp_creacion")
            distribucion = datos.get("distribucion_origen", "desconocida")
            intentos_previos = datos.get("intentos", 0)
            
            # --- PROCESAMIENTO SIN BLOQUEOS (ASÍNCRONO REAL) ---
            try:
                respuesta = requests.get(f"{CACHE_URL}/{tipo_consulta}", params=parametros, timeout=5)
                respuesta.raise_for_status() 
                
                resultado = respuesta.json()
                origen_respuesta = resultado.get("origen", "Desconocido")
                
                print(f"✅ [PROCESADO] {tipo_consulta} | Origen: {origen_respuesta} | Intento: {intentos_previos + 1}")
                
                t_procesado = time.time()
                guardar_metrica(consulta_id, t_creacion, t_procesado, origen_respuesta, distribucion)
                
            except requests.exceptions.RequestException as e:
                intentos_actuales = intentos_previos + 1
                datos["intentos"] = intentos_actuales
                
                if intentos_actuales < MAX_REINTENTOS:
                    print(f"⚠️ [FALLO TEMPORAL] Intento {intentos_actuales}/{MAX_REINTENTOS}. Enviando a Tópico de Reintentos...")
                    producer.produce(TOPIC_REINTENTOS, value=json.dumps(datos).encode('utf-8'), callback=delivery_report)
                else:
                    print(f"🚨 [CRÍTICO] Máximo de intentos alcanzado. Enviando a DLQ...")
                    datos["razon_fallo"] = "Caída del Cache/Generador"
                    datos["timestamp_dlq"] = time.time()
                    producer.produce(TOPIC_DLQ, value=json.dumps(datos).encode('utf-8'), callback=delivery_report)
            
            producer.poll(0)
            time.sleep(0.01)

    except KeyboardInterrupt:
        print("Apagando consumidor...")
    finally:
        producer.flush() 
        consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()
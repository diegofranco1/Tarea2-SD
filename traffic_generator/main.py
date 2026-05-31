import time
import random
import numpy as np
import sys
import json
import uuid
from confluent_kafka import Producer

# ==========================================
# CONFIGURACIÓN KAFKA Y CONSTANTES
# ==========================================
KAFKA_BROKER = "kafka:9092"
TOPIC_PRINCIPAL = "consultas_topic"

ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]
NUM_REQUESTS = 1000 

def generar_zona_zipf():
    while True:
        z = np.random.zipf(1.5)
        if z <= 5:
            return ZONAS[z-1]

def delivery_report(err, msg):
    """ Función que nos avisa si hubo un error real al entregar el mensaje """
    if err is not None:
        print(f'ERROR: No se pudo entregar el mensaje: {err}')

def ejecutar_trafico(distribucion="uniforme"):
    print("Conectando con la oficina de correos (Kafka, Vía Confluent)...")
    
    # Configuración oficial de Confluent
    # Configuración oficial de Confluent
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'message.timeout.ms': 10000  # NUEVO: Si falla, aborta a los 10s y no se queda pegado
    }
    producer = Producer(conf)
    
    print("¡Conexión exitosa a Kafka!")
    print(f"--- Iniciando bombardeo ASÍNCRONO con distribución: {distribucion.upper()} ---")
    
    for i in range(NUM_REQUESTS):
        q = random.choice(CONSULTAS)
        
        if distribucion == "zipf":
            zona_principal = generar_zona_zipf()
        else:
            zona_principal = random.choice(ZONAS)
            
        if q == "q4":
            zona_secundaria = random.choice(ZONAS)
            while zona_secundaria == zona_principal:
                zona_secundaria = random.choice(ZONAS)
            
            params = {
                "zona_a": zona_principal,      # <-- ¡Cambio aquí!
                "zona_b": zona_secundaria,     # <-- ¡Cambio aquí!
                "confidence_min": round(random.uniform(0.0, 1.0), 3)
            }
            zona_log = f"{zona_principal} vs {zona_secundaria}"
        else:
            params = {
                "zona_id": zona_principal, 
                "confidence_min": round(random.uniform(0.0, 1.0), 3)
            }
            zona_log = zona_principal
            
        mensaje = {
            "id_consulta": str(uuid.uuid4()),
            "consulta": q,
            "parametros": params,
            "timestamp_creacion": time.time(),
            "intentos": 0,
            "distribucion_origen": distribucion
        }
        
        # Empaquetamos en JSON
        valor_json = json.dumps(mensaje).encode('utf-8')
        
        # Disparamos el mensaje a Kafka
        producer.produce(TOPIC_PRINCIPAL, value=valor_json, callback=delivery_report)
        
        # poll() limpia la memoria de confirmaciones
        producer.poll(0)
        
        print(f"Mensaje {i+1} enviado a Kafka | {q} en {zona_log}")
        
        time.sleep(0.01) 

    # Esperamos a que los últimos mensajes terminen de viajar por la red
    print("Asegurando la entrega final de todos los mensajes...")
    producer.flush()
    print(f"¡Bombardeo finalizado! {NUM_REQUESTS} mensajes entregados a la cola '{TOPIC_PRINCIPAL}'.")

if __name__ == "__main__":
    print("Preparando Productor Kafka... esperando 10 segundos a que el broker despierte.")
    time.sleep(10)
    
    tipo_dist = sys.argv[1] if len(sys.argv) > 1 else "uniforme"
    ejecutar_trafico(tipo_dist)
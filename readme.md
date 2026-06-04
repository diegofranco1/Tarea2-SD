# Plataforma Asíncrona de Consultas Geoespaciales con Apache Kafka

**Curso:** Sistemas Distribuidos - Universidad Diego Portales
**Autores:** Diego Franco y Vicente Calderón
**Video demostrativo:** [https://www.youtube.com/watch?v=nlM-0HEHrXI]

## Descripción

Este proyecto corresponde a la segunda iteración de una plataforma distribuida para el procesamiento de consultas geoespaciales utilizando el dataset Google Open Buildings.

A diferencia de la primera entrega, basada principalmente en comunicación síncrona entre servicios, esta versión incorpora **Apache Kafka** como sistema de mensajería para desacoplar componentes, mejorar la tolerancia a fallos y permitir el procesamiento paralelo mediante múltiples consumidores.

El objetivo principal es evaluar cómo una arquitectura orientada a eventos responde frente a escenarios de alta concurrencia, fallas temporales y picos de tráfico, manteniendo la integridad de las consultas y reduciendo la pérdida de información.

## Arquitectura General

La plataforma está compuesta por varios microservicios que colaboran de forma asíncrona mediante Kafka.

### Flujo normal de procesamiento

1. El generador de tráfico produce consultas geoespaciales.
2. Las consultas son publicadas en un tópico principal de Kafka.
3. Los consumidores procesan los mensajes de forma paralela.
4. Se consulta primero la caché Redis.
5. Si ocurre un *cache miss*, la solicitud es enviada al Generador de Respuestas.
6. El resultado se almacena nuevamente en caché para futuras consultas.

### Mecanismo de resiliencia

Cuando ocurre una falla temporal durante el procesamiento:

* El mensaje no se pierde.
* Se envía automáticamente a un tópico de reintentos.
* El sistema vuelve a procesarlo más adelante.

Si una consulta supera el máximo de tres intentos configurados:

* El mensaje se deriva a una **Dead Letter Queue (DLQ)**.
* Queda disponible para análisis y auditoría posterior.

## Estructura del Proyecto

* `cache_system/` → Servicio de caché basado en Redis y FastAPI.
* `kafka_consumer/` → Consumidores encargados de procesar consultas y gestionar reintentos.
* `response_generator/` → Servicio que ejecuta el procesamiento geoespacial.
* `traffic_generator/` → Productor encargado de generar e inyectar carga al sistema.
* `analisis.py` → Script para analizar resultados experimentales y generar gráficas.
* `docker-compose.yml` → Configuración completa de la infraestructura distribuida.

## Tecnologías Utilizadas

* Apache Kafka
* Zookeeper
* Redis
* Docker
* Docker Compose
* FastAPI
* Python

## Ejecución

### 1. Levantar la infraestructura

```bash
docker compose up --build -d
```

Este comando inicia:

* Kafka
* Zookeeper
* Redis
* Sistema de Caché
* Generador de Respuestas
* Consumidores
* Generador de Tráfico

### 2. Verificar contenedores

```bash
docker ps
```

### 3. Ejecutar escenarios experimentales

Dependiendo del escenario que se quiera evaluar, pueden ejecutarse los scripts de generación de carga y simulación de fallas descritos en el informe.

## Escenarios Evaluados

El proyecto fue diseñado para evaluar los siguientes escenarios:

* Arquitectura síncrona base.
* Kafka con un único consumidor.
* Kafka con múltiples consumidores.
* Falla temporal del backend.
* Falla prolongada y uso de DLQ.
* Spike de tráfico.
* Recuperación automática y vaciado del backlog.

## Objetivos Alcanzados

* Desacoplamiento entre productor y consumidores.
* Procesamiento asíncrono basado en eventos.
* Reintentos automáticos ante fallas temporales.
* Implementación de Dead Letter Queue.
* Escalamiento horizontal mediante grupos de consumo.
* Absorción de picos de tráfico mediante Kafka.
* Medición de throughput, latencia, backlog y recovery time.












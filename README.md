# Procesamiento de Datos con Apache Spark y Apache Kafka (Batch & Streaming)

* **Estudiante:** Emanuel Martínez Pinzón
* **Curso:** Big Data (Código: 202016911)
* **Grupo:** 50
* **Tutor:** Ing. Fabián Alfredo García Rincón
* **Universidad Nacional Abierta y a Distancia – UNAD**
* **Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)**

---

## 1. Descripción del Proyecto
Este repositorio contiene la implementación práctica y funcional de la Tarea 3 - Procesamiento de Datos con Apache Spark. Se abordan dos paradigmas fundamentales de la analítica distribuida a gran escala:
1. Procesamiento por Lotes (Batch): Limpieza, transformación y análisis exploratorio de datos (EDA) sobre un conjunto masivo de datos de salud pública utilizando DataFrames en PySpark.
2. Procesamiento en Tiempo Real (Streaming): Ingesta continua de eventos mediante un clúster de Apache Kafka y procesamiento analítico de flujos en tiempo real con Apache Spark Streaming (Structured Streaming) aplicando agregaciones por ventanas temporales.

---

## 2. Tecnologías y Entorno de Ejecución
* Sistema Operativo: Ubuntu Server 22.04 LTS (VirtualBox).
* Motor de Cómputo Distribuido: Apache Spark 3.5.x (PySpark API).
* Plataforma de Mensajería y Streaming: Apache Kafka 3.6.x coordinado por Apache ZooKeeper.
* Lenguaje de Programación: Python 3.10+.
* Monitoreo: Spark Web UI (Puerto 4040).

---

## 3. Estructura del Repositorio
* tarea3_batch.py: Script de PySpark para la lectura, estandarización de esquemas, estadísticas descriptivas y consultas de agregación sobre el dataset masivo.
* covid19_colombia.csv: Conjunto de datos representativo utilizado para el análisis en lote.
* kafka_producer.py: Productor en Python que simula lecturas de sensores IoT y envía eventos JSON de forma continua hacia un tópico de Kafka.
* spark_streaming_consumer.py: Consumidor desarrollado en Spark Streaming que lee el tópico de Kafka, parsea la estructura de datos y calcula métricas agregadas por ventanas deslizantes de tiempo.
* README.md: Documentación técnica y guía de ejecución del proyecto.

---

## 4. Componente 1: Procesamiento por Lotes (Batch)

### Conjunto de Datos
* Nombre: Casos positivos de COVID-19 en Colombia.
* Fuente: Instituto Nacional de Salud (INS) – Portal de Datos Abiertos (datos.gov.co).
* Enlace Oficial: https://www.datos.gov.co/d/gt2j-8ykr

### Ejecución del Análisis Batch
Comando para ejecutar el script:
python3 tarea3_batch.py

### Resultados Obtenidos
* Volumen: Carga e inferencia de esquema sobre 50.000 registros estructurados.
* Estadística Descriptiva: Media de edad calculada en 40.44 años (desviación estándar de 17.99 y rango de 1 a 107 años).
* Distribución Territorial: Agregación descendente por departamento identificando mayor incidencia en Bogotá (13.648 casos), Antioquia (7.403) y Valle del Cauca (4.148).
* Condición de Severidad: Filtrado y segmentación de casos críticos en condición de hospitalización grave o fallecimiento.
* Equilibrio Demográfico: Distribución de contagios por género (Femenino: 25.119 / Masculino: 24.881) con medias de edad homogéneas.

---

## 5. Componente 2: Procesamiento en Tiempo Real (Kafka & Spark Streaming)

### Arquitectura de Flujo
1. ZooKeeper & Broker Kafka: Servicio de mensajería distribuida escuchando en el puerto local 9092 con el tópico particionado sensor_data.
2. Productor (kafka_producer.py): Genera periódicamente registros de telemetría IoT con identificador de sensor (sensor_id), mediciones cuantitativas (temperature, humidity) y marca temporal (timestamp).
3. Consumidor (spark_streaming_consumer.py): Suscripción al tópico sensor_data mediante el conector spark-sql-kafka. Parsea la carga JSON y calcula promedios de temperatura y humedad agrupados por sensor en ventanas temporales de 1 minuto.

### Instrucciones de Ejecución

1. Iniciar el clúster Kafka y ZooKeeper (en segundo plano):
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

2. Crear el tópico de eventos:
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data

3. Ejecutar el Productor de Eventos (Terminal 1):
python3 kafka_producer.py

4. Ejecutar el Consumidor de Spark Streaming (Terminal 2):
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py

5. Monitoreo en Spark Web UI:
Acceder a http://localhost:4040 (o a la dirección IP asignada a la máquina virtual) para verificar las métricas activas de Structured Streaming, la tasa de procesamiento de micro-lotes (batches) y la ejecución de tareas en los executors.

# Procesamiento de Datos Masivos con Apache Spark (Batch)

**Estudiante:** Emanuel Martínez Pinzón
**Curso:** Big Data (Código: 202016911)
**Grupo:** 50
**Tutor:** Ing. Fabián Alfredo García Rincón
**Universidad Nacional Abierta y a Distancia – UNAD**
**Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)**

---

## 1. Descripción del Proyecto
Este repositorio contiene la implementación práctica del componente de procesamiento por lotes (batch) correspondiente a la Tarea 3 - Procesamiento de Datos con Apache Spark. Se desarrolla un análisis exploratorio de datos (EDA), estandarización de esquemas y evaluación de métricas epidemiológicas a partir de un conjunto de datos masivo de Colombia.

## 2. Conjunto de Datos
* **Nombre:** Casos positivos de COVID-19 en Colombia
* **Entidad / Fuente:** Instituto Nacional de Salud (INS) – Portal de Datos Abiertos del Estado Colombiano (datos.gov.co)
* **Enlace Oficial:** https://www.datos.gov.co/d/gt2j-8ykr
* **Formato:** Archivo delimitado por comas (.csv) procesado con inferencia automática de esquemas tipados.

## 3. Tecnologías y Herramientas Utilizadas
* **Sistema Operativo:** Ubuntu Server 22.04 LTS (Entorno virtualizado en VirtualBox)
* **Motor de Cómputo Distribuido:** Apache Spark 3.5.x
* **Lenguaje:** Python 3.10+ / API PySpark
* **Abstracción:** DataFrames de Spark con motor de optimización Catalyst y gestión en memoria Tungsten

## 4. Estructura del Repositorio
* tarea3_batch.py: Script de PySpark con las etapas de carga, limpieza, casteo de tipos, estadísticas descriptivas y consultas analíticas.
* covid19_colombia.csv: Conjunto de datos representativo utilizado para la ejecución distribuida.
* README.md: Documentación formal del proyecto e instrucciones de ejecución técnica.

## 5. Instrucciones de Ejecución
Para clonar y reproducir este análisis en un entorno con Apache Spark configurado:

1. Clonar el repositorio público:
   git clone https://github.com/TU_USUARIO/big-data-tarea3-spark-emanuel.git
   cd big-data-tarea3-spark-emanuel

2. Ejecutar el script batch:
   python3 tarea3_batch.py

## 6. Resultados Obtenidos del Análisis Exploratorio (EDA)
* **Carga de Datos:** Procesamiento distribuido de 50.000 registros tabulares.
* **Estadística Descriptiva:** Cálculo de media de edad (40.44 años), desviación estándar (17.99) y rango demográfico (1 a 107 años).
* **Distribución Geográfica:** Agrupación y ordenamiento descendente de los departamentos con mayor concentración de casos (Bogotá, Antioquia, Valle, Cundinamarca, Santander).
* **Severidad:** Filtrado condicional de casos de alta criticidad en condición 'Fallecido' o 'Grave'.
* **Métricas por Género:** Distribución cuantitativa y media de edad desagregada por sexo (Femenino: 25.119 registros / Masculino: 24.881 registros).

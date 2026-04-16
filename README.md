# Tarea 3 - Procesamiento de Datos con Apache Spark
**Big Data - 202016911 | UNAD**
**Estudiante:** Jair Andres Galvan Fernandez
**Grupo:** 58
**Tutor:** Handry Orozco Collazos

## Descripcion general
Este repositorio contiene el desarrollo individual de la Tarea 3 del curso Big Data, orientado al procesamiento y analisis de grandes volumenes de datos usando Apache Spark (Python) y Apache Kafka.

## Dataset utilizado
- **Nombre:** Online Retail Sales and Customer Data
- **Archivo:** online_retail.csv
- **Fuente:** Kaggle - Szafraniec, M. (2023)
- **URL:** https://www.kaggle.com/datasets/thedevastator/online-retail-sales-and-customer-data/data

## Problema definido
Analizar el comportamiento de compra de los clientes para identificar patrones de demanda por producto y segmentos de clientes con alta frecuencia de compra, apoyando estrategias de gestion de inventario y retencion de clientes.

## Archivos
| Archivo | Descripcion |
|---|---|
| `Tarea3_batch.py` | Procesamiento batch con Spark: carga, limpieza, transformacion y analisis EDA |
| `Tarea3_kafka_producer.py` | Productor Kafka: simula llegada de datos en tiempo real |
| `Tarea3_streaming_consumer.py` | Consumidor Spark Streaming: analisis en tiempo real |
| `instrucciones_ejecucion.md` | Pasos para ejecutar los scripts |

## Instrucciones de ejecucion
### Requisitos
- Python 3.8 o superior
- PySpark: `pip install pyspark`
- matplotlib: `pip install matplotlib`
- Dataset `online_retail.csv` en la misma carpeta del script

### Opcion A - Google Colab (recomendada)
1. Subir el archivo `online_retail.csv` a Google Colab.
2. Instalar PySpark: `!pip install pyspark`
3. Ejecutar el script `Tarea3_batch.py`

### Opcion B - Entorno local
1. Instalar Java 8 o superior.
2. Instalar PySpark: `pip install pyspark`
3. Ejecutar: `python Tarea3_batch.py`

## Tecnologias usadas
- Python 3.x
- Apache Spark (PySpark)
- Apache Kafka
- Google Colab

## Referencias
Szafraniec, M. (2023). Online Retail Sales and Customer Data [Dataset]. Kaggle.
https://www.kaggle.com/datasets/thedevastator/online-retail-sales-and-customer-data/data

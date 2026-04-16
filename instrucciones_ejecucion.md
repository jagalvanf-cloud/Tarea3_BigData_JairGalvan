# Instrucciones de Ejecucion - Tarea 3 Big Data UNAD

**Estudiante:** Jair Andres Galvan Fernandez  
**Grupo:** 58 | **Curso:** Big Data - 202016911  
**Tutor:** Handry Orozco Collazos

---

## Requisitos previos

- Python 3.8 o superior
- Java 8 o superior (requerido por Spark)
- PySpark: `pip install pyspark`
- matplotlib: `pip install matplotlib`
- Dataset `online_retail.csv` en la misma carpeta del script
  - Fuente: https://www.kaggle.com/datasets/thedevastator/online-retail-sales-and-customer-data/data

---

## Opcion A - Google Colab (RECOMENDADA)

Esta es la forma mas sencilla de ejecutar el proyecto sin instalar nada localmente.

### Pasos:

1. Abrir [Google Colab](https://colab.research.google.com/)
2. Subir el archivo `online_retail.csv` al entorno de Colab
3. Instalar PySpark:
   ```
   !pip install pyspark
   !pip install matplotlib
   ```
4. Subir y ejecutar el script `Tarea3_batch.py`
5. Para el productor Kafka (simulado): subir y ejecutar `Tarea3_kafka_producer.py`

---

## Opcion B - Entorno local

### Paso 1: Instalar Java
- Descargar Java 8+ desde https://adoptium.net/
- Verificar: `java -version`

### Paso 2: Instalar dependencias
```bash
pip install pyspark
pip install matplotlib
```

### Paso 3: Descargar el dataset
- Descargar `online_retail.csv` desde Kaggle
- Colocarlo en la misma carpeta que los scripts

### Paso 4: Ejecutar procesamiento batch
```bash
python Tarea3_batch.py
```

### Paso 5: Ejecutar productor Kafka (simulado)
```bash
python Tarea3_kafka_producer.py
```

### Paso 6: Ejecutar consumidor Streaming
```bash
# Crear carpeta stream_data/ y copiar fragmentos del CSV ahi
mkdir stream_data
python Tarea3_streaming_consumer.py
```

---

## Salidas esperadas

### Tarea3_batch.py
- Estadisticas del dataset
- Top 10 productos mas vendidos
- Ventas por pais (tabla)
- Top 10 clientes
- Graficas PNG en carpeta `graficas/`:
  - `top_productos.png`
  - `ventas_pais.png`

### Tarea3_kafka_producer.py
- Imprime 50 mensajes JSON simulando envio a Kafka
- Cada mensaje contiene datos de una transaccion del retail

### Tarea3_streaming_consumer.py
- Lee datos en modo streaming
- Muestra agregaciones por pais cada 10 segundos
- Se detiene automaticamente despues de 60 segundos

---

## Notas tecnicas

- El productor Kafka es una **simulacion** (no requiere broker Kafka instalado)
- Para usar Kafka real, instalar `kafka-python`: `pip install kafka-python`
- El streaming usa modo de archivos como fuente para compatibilidad con Colab
- En produccion real, configurar con `spark.readStream.format('kafka')`

---

## Referencias

- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- PySpark API: https://spark.apache.org/docs/latest/api/python/
- Apache Kafka: https://kafka.apache.org/documentation/
- Dataset: Szafraniec, M. (2023). Online Retail Sales and Customer Data. Kaggle.

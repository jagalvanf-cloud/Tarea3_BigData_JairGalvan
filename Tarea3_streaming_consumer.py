# Tarea 3 - Procesamiento de Datos con Apache Spark
# Big Data - UNAD 202016911
# Estudiante: Jair Andres Galvan Fernandez
# Script: Consumidor Spark Streaming - Analisis en tiempo real
# Simulacion de consumidor de stream usando PySpark Structured Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as spark_sum, count, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

# ============================================================
# 1. INICIALIZACION DE SPARK
# ============================================================
spark = SparkSession.builder \
    .appName("Tarea3_Streaming_JairGalvan") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("SparkSession Streaming iniciada.")

# ============================================================
# 2. ESQUEMA DEL MENSAJE KAFKA (JSON)
# ============================================================
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# ============================================================
# 3. SIMULACION DE STREAM DESDE ARCHIVOS
# (En entorno real Kafka: spark.readStream.format('kafka'))
# ============================================================
print("[STREAMING] Iniciando lectura de stream simulado desde archivos CSV...")

# Leer CSV como stream (modo rate para simulacion)
df_stream = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .csv("stream_data/")

# ============================================================
# 4. TRANSFORMACIONES EN TIEMPO REAL
# ============================================================
# Calcular TotalVenta por registro
df_procesado = df_stream.filter(
    (col("Quantity") > 0) & (col("UnitPrice") > 0)
).withColumn(
    "TotalVenta", col("Quantity") * col("UnitPrice")
)

# Agregacion: ventas por pais en ventana de tiempo
ventas_pais = df_procesado.groupBy("Country") \
    .agg(
        spark_sum("TotalVenta").alias("VentaAcumulada"),
        count("InvoiceNo").alias("NumTransacciones"),
        avg("UnitPrice").alias("PrecioPromedio")
    )

# ============================================================
# 5. SALIDA DEL STREAM (CONSOLA)
# ============================================================
print("[STREAMING] Escribiendo resultados en consola...")
print("[STREAMING] Para detener: Ctrl+C")
print("-" * 60)

query = ventas_pais.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# Ejecutar por 60 segundos o hasta interrupcion
try:
    query.awaitTermination(timeout=60)
except Exception as e:
    print(f"[STREAMING] Stream finalizado: {e}")
finally:
    print("[STREAMING] Deteniendo SparkSession.")
    spark.stop()

print("Procesamiento streaming completado.")

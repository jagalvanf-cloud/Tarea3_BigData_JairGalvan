# Tarea 3 - Procesamiento de Datos con Apache Spark
# Big Data - UNAD 202016911
# Estudiante: Jair Andres Galvan Fernandez
# Script: Procesamiento Batch con PySpark
# Dataset: Online Retail Sales and Customer Data (Kaggle)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, month, year, desc, round as spark_round
from pyspark.sql.types import DoubleType
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

# ============================================================
# 1. INICIALIZACION DE SPARK
# ============================================================
spark = SparkSession.builder \
    .appName("Tarea3_BigData_JairGalvan") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("SparkSession iniciada correctamente.")

# ============================================================
# 2. CARGA DEL DATASET
# ============================================================
df = spark.read.csv("online_retail.csv", header=True, inferSchema=True)
print(f"Registros cargados: {df.count()}")
df.printSchema()
df.show(5)

# ============================================================
# 3. LIMPIEZA Y TRANSFORMACION
# ============================================================
# Eliminar nulos en columnas clave
df_clean = df.dropna(subset=["CustomerID", "InvoiceNo", "StockCode", "Quantity", "UnitPrice"])

# Filtrar registros con valores negativos o cero
df_clean = df_clean.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))

# Crear columna TotalVenta
df_clean = df_clean.withColumn("TotalVenta", spark_round(col("Quantity") * col("UnitPrice"), 2))

print(f"Registros despues de limpieza: {df_clean.count()}")

# ============================================================
# 4. ANALISIS EXPLORATORIO (EDA)
# ============================================================

# 4.1 Top 10 productos mas vendidos (por cantidad)
print("\n--- Top 10 Productos mas vendidos ---")
top_productos = df_clean.groupBy("Description") \
    .agg(spark_sum("Quantity").alias("TotalCantidad")) \
    .orderBy(desc("TotalCantidad")) \
    .limit(10)
top_productos.show()

# 4.2 Ventas totales por pais
print("\n--- Ventas Totales por Pais ---")
ventas_pais = df_clean.groupBy("Country") \
    .agg(spark_round(spark_sum("TotalVenta"), 2).alias("VentaTotal")) \
    .orderBy(desc("VentaTotal")) \
    .limit(10)
ventas_pais.show()

# 4.3 Clientes con mayor gasto total
print("\n--- Top 10 Clientes con mayor gasto ---")
top_clientes = df_clean.groupBy("CustomerID") \
    .agg(spark_round(spark_sum("TotalVenta"), 2).alias("GastoTotal")) \
    .orderBy(desc("GastoTotal")) \
    .limit(10)
top_clientes.show()

# 4.4 Estadisticas generales
print("\n--- Estadisticas generales ---")
df_clean.select("Quantity", "UnitPrice", "TotalVenta").describe().show()

# ============================================================
# 5. VISUALIZACIONES
# ============================================================
os.makedirs("graficas", exist_ok=True)

# Grafica Top 10 productos
productos_pd = top_productos.toPandas()
plt.figure(figsize=(12, 6))
plt.barh(productos_pd["Description"], productos_pd["TotalCantidad"], color='steelblue')
plt.xlabel("Cantidad Vendida")
plt.title("Top 10 Productos mas Vendidos")
plt.tight_layout()
plt.savefig("graficas/top_productos.png")
plt.close()
print("Grafica top_productos.png generada.")

# Grafica ventas por pais
paises_pd = ventas_pais.toPandas()
plt.figure(figsize=(12, 6))
plt.bar(paises_pd["Country"], paises_pd["VentaTotal"], color='coral')
plt.xlabel("Pais")
plt.ylabel("Venta Total (GBP)")
plt.title("Top 10 Paises por Ventas")
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig("graficas/ventas_pais.png")
plt.close()
print("Grafica ventas_pais.png generada.")

# ============================================================
# 6. CIERRE
# ============================================================
print("\nProcesamiento batch completado exitosamente.")
spark.stop()

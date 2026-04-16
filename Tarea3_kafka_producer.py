# Tarea 3 - Procesamiento de Datos con Apache Spark
# Big Data - UNAD 202016911
# Estudiante: Jair Andres Galvan Fernandez
# Script: Productor Kafka - Simulacion de datos en tiempo real
# Dataset: Online Retail Sales and Customer Data (Kaggle)

import json
import time
import random
import csv
from datetime import datetime

# Simulacion de productor Kafka sin broker real
# En entorno con Kafka instalado, usar: from kafka import KafkaProducer

TOPIC = "retail_ventas"
DATA_FILE = "online_retail.csv"

def simular_productor(archivo, intervalo=0.5, max_mensajes=100):
    """
    Simula un productor Kafka leyendo filas del CSV
    y enviandolas como mensajes JSON al topico.
    """
    print(f"[KAFKA PRODUCER] Iniciando envio al topico: {TOPIC}")
    print(f"[KAFKA PRODUCER] Leyendo datos de: {archivo}")
    print("-" * 60)

    mensajes_enviados = 0

    try:
        with open(archivo, encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if mensajes_enviados >= max_mensajes:
                    break

                # Construir mensaje
                mensaje = {
                    "timestamp": datetime.now().isoformat(),
                    "InvoiceNo": row.get("InvoiceNo", ""),
                    "StockCode": row.get("StockCode", ""),
                    "Description": row.get("Description", ""),
                    "Quantity": int(row.get("Quantity", 0) or 0),
                    "InvoiceDate": row.get("InvoiceDate", ""),
                    "UnitPrice": float(row.get("UnitPrice", 0.0) or 0.0),
                    "CustomerID": row.get("CustomerID", ""),
                    "Country": row.get("Country", "")
                }

                mensaje_json = json.dumps(mensaje)
                print(f"[MSG {mensajes_enviados+1}] -> {mensaje_json[:80]}...")

                # En entorno real: producer.send(TOPIC, value=mensaje_json.encode('utf-8'))
                mensajes_enviados += 1
                time.sleep(intervalo)

    except FileNotFoundError:
        print(f"ERROR: No se encontro el archivo {archivo}")
        print("Por favor coloque online_retail.csv en el mismo directorio.")
        return

    print("-" * 60)
    print(f"[KAFKA PRODUCER] Total mensajes enviados: {mensajes_enviados}")
    print("[KAFKA PRODUCER] Finalizando productor.")


if __name__ == "__main__":
    print("=" * 60)
    print("PRODUCTOR KAFKA - Tarea 3 Big Data UNAD")
    print("Estudiante: Jair Andres Galvan Fernandez")
    print("=" * 60)
    simular_productor(DATA_FILE, intervalo=0.3, max_mensajes=50)

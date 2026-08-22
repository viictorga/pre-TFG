"""
Fase 3 (parte A): Spark Structured Streaming lee los eventos CDC del topic
de Kafka y los muestra por consola.

Todavía NO escribe en Iceberg -- eso es la parte B, el siguiente paso, una
vez esto quede confirmado. El objetivo aquí es solo demostrar que Spark
sabe leer el stream de Kafka y entender el formato de evento de Debezium.

Ejecutar dentro del contenedor spark-iceberg:

    docker exec -it tfg-spark spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
        /home/iceberg/scripts/read_kafka_console.py

La primera vez tardará un poco más: Spark tiene que descargar el conector
de Kafka (el flag --packages) desde Maven Central.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("LeerEventosCDC").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema del campo "after" del evento de Debezium: el estado del
# dispositivo tras el cambio. Coincide con la tabla estado_dispositivo.
after_schema = StructType([
    StructField("id_dispositivo", StringType()),
    StructField("tipo_sensor", StringType()),
    StructField("ubicacion", StringType()),
    StructField("valor", DoubleType()),
    StructField("unidad", StringType()),
    StructField("estado", StringType()),
    StructField("fecha_actualizacion", StringType()),
])

# Solo modelamos "payload.after" y "payload.op" -- el resto del envelope
# (schema, source, before...) lo ignoramos por ahora sin que dé error:
# from_json simplemente descarta los campos que no le pedimos.
envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("after", after_schema),
        StructField("op", StringType()),
    ])),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "iot.public.estado_dispositivo")
    .option("startingOffsets", "earliest")
    .load()
)

# El "value" de cada mensaje de Kafka llega en bytes; lo convertimos a
# texto y extraemos el estado del dispositivo tras el cambio.
# .\venv\Scripts\Activate.ps1
eventos = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), envelope_schema).alias("evento"))
    .select(
        col("evento.payload.op").alias("operacion"),
        col("evento.payload.after.id_dispositivo"),
        col("evento.payload.after.tipo_sensor"),
        col("evento.payload.after.ubicacion"),
        col("evento.payload.after.valor"),
        col("evento.payload.after.unidad"),
    )
)

query = (
    eventos.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()

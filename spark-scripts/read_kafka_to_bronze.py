"""
Fase 3B: Spark Structured Streaming escribe los eventos de transacciones
en la capa bronze del lakehouse (tabla Iceberg demo.bronze.eventos_cuenta),
en vez de solo imprimirlos.

Igual que con los sensores: "estado_cuenta" en Postgres solo guarda la
ULTIMA transaccion de cada cuenta (hace UPSERT), pero esta tabla bronze
guarda TODAS las transacciones, una por una -- el historial completo que
Postgres por si solo no conserva, y sobre el que luego se calculan las
estadisticas de deteccion de fraude.

Ejecutar dentro del contenedor spark-iceberg (todo en una sola linea):

    docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/read_kafka_to_bronze.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("EscribirBronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.bronze.eventos_cuenta (
        id_cuenta STRING,
        tipo_transaccion STRING,
        categoria_comercio STRING,
        canal STRING,
        ubicacion STRING,
        importe DOUBLE,
        moneda STRING,
        resultado STRING,
        estado STRING,
        fecha_actualizacion STRING,
        operacion STRING,
        fecha_ingesta TIMESTAMP
    ) USING iceberg
""")

after_schema = StructType([
    StructField("id_cuenta", StringType()),
    StructField("tipo_transaccion", StringType()),
    StructField("categoria_comercio", StringType()),
    StructField("canal", StringType()),
    StructField("ubicacion", StringType()),
    StructField("importe", DoubleType()),
    StructField("moneda", StringType()),
    StructField("resultado", StringType()),
    StructField("estado", StringType()),
    StructField("fecha_actualizacion", StringType()),
])

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
    .option("subscribe", "fraude.public.estado_cuenta")
    .option("startingOffsets", "earliest")
    .load()
)

eventos = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), envelope_schema).alias("evento"))
    .select(
        col("evento.payload.after.id_cuenta"),
        col("evento.payload.after.tipo_transaccion"),
        col("evento.payload.after.categoria_comercio"),
        col("evento.payload.after.canal"),
        col("evento.payload.after.ubicacion"),
        col("evento.payload.after.importe"),
        col("evento.payload.after.moneda"),
        col("evento.payload.after.resultado"),
        col("evento.payload.after.estado"),
        col("evento.payload.after.fecha_actualizacion"),
        col("evento.payload.op").alias("operacion"),
        current_timestamp().alias("fecha_ingesta"),
    )
    .filter(col("id_cuenta").isNotNull())
)

query = (
    eventos.writeStream
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/bronze_eventos_cuenta")
    .toTable("demo.bronze.eventos_cuenta")
)

query.awaitTermination()
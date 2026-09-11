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

from metricas import RegistradorDeMetricas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

spark = SparkSession.builder.appName("EscribirBronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Registra una fila de metricas por micro-lote (ver metricas.py).
spark.streams.addListener(RegistradorDeMetricas("bronze"))

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
        es_anomalia_generada BOOLEAN,
        tipo_anomalia_generada STRING,
        operacion STRING,
        fecha_ingesta TIMESTAMP
    ) USING iceberg
""")


def asegurar_columnas(spark, tabla, columnas):
    """Anade a una tabla Iceberg ya existente las columnas que le falten.

    CREATE TABLE IF NOT EXISTS no toca una tabla que ya existe, asi que una
    tabla creada antes de anadir la etiqueta de verdad se quedaria sin esas
    columnas y el append fallaria. Iceberg permite evolucionar el esquema sin
    reescribir los datos: las filas antiguas devuelven NULL en la columna
    nueva.
    """
    existentes = {c.lower() for c in spark.table(tabla).columns}
    for nombre, tipo in columnas:
        if nombre.lower() not in existentes:
            spark.sql(f"ALTER TABLE {tabla} ADD COLUMN {nombre} {tipo}")
            print(f"Esquema evolucionado: {tabla} + {nombre} {tipo}")


asegurar_columnas(spark, "demo.bronze.eventos_cuenta", [
    ("es_anomalia_generada", "BOOLEAN"),
    ("tipo_anomalia_generada", "STRING"),
])

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
    # Etiqueta de verdad que inyecta el generador. Viaja por el pipeline para
    # poder evaluar la deteccion a posteriori, pero la logica de deteccion
    # NO puede usarla.
    StructField("es_anomalia_generada", BooleanType()),
    StructField("tipo_anomalia_generada", StringType()),
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
        col("evento.payload.after.es_anomalia_generada"),
        col("evento.payload.after.tipo_anomalia_generada"),
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
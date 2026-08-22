"""
Fase 3 (parte B): Spark Structured Streaming escribe los eventos CDC en la
capa bronze del lakehouse (una tabla Iceberg real), en vez de solo
imprimirlos por consola.

A diferencia de "estado_dispositivo" en Postgres (que solo guarda el
ultimo valor de cada sensor, porque cada lectura hace UPSERT), esta tabla
bronze guarda TODOS los eventos, uno por cada cambio: es el historial
completo que Postgres por si solo no conserva.

Ejecutar dentro del contenedor spark-iceberg (todo en una sola linea):

    docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/read_kafka_to_bronze.py

La imagen spark-iceberg ya trae configurado un catalogo Iceberg llamado
"demo" que apunta a MinIO + el catalogo REST -- no hace falta configurar
nada de eso aqui, solo usarlo.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("EscribirBronze").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.bronze.eventos_dispositivo (
        id_dispositivo STRING,
        tipo_sensor STRING,
        ubicacion STRING,
        valor DOUBLE,
        unidad STRING,
        estado STRING,
        fecha_actualizacion STRING,
        operacion STRING,
        fecha_ingesta TIMESTAMP
    ) USING iceberg
""")

after_schema = StructType([
    StructField("id_dispositivo", StringType()),
    StructField("tipo_sensor", StringType()),
    StructField("ubicacion", StringType()),
    StructField("valor", DoubleType()),
    StructField("unidad", StringType()),
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
    .option("subscribe", "iot.public.estado_dispositivo")
    .option("startingOffsets", "earliest")
    .load()
)

eventos = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), envelope_schema).alias("evento"))
    .select(
        col("evento.payload.after.id_dispositivo"),
        col("evento.payload.after.tipo_sensor"),
        col("evento.payload.after.ubicacion"),
        col("evento.payload.after.valor"),
        col("evento.payload.after.unidad"),
        col("evento.payload.after.estado"),
        col("evento.payload.after.fecha_actualizacion"),
        col("evento.payload.op").alias("operacion"),
        current_timestamp().alias("fecha_ingesta"),
    )
    # Descarta eventos sin "after" (por ejemplo, un DELETE, que no generamos
    # en este proyecto pero es buena práctica filtrarlo de todos modos)
    .filter(col("id_dispositivo").isNotNull())
)

query = (
    eventos.writeStream
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/bronze_eventos_dispositivo")
    .toTable("demo.bronze.eventos_dispositivo")
)

query.awaitTermination()

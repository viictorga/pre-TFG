"""
Fase 3C: Deteccion de anomalias en tiempo real por ventana deslizante.

Para cada lectura que llega, calcula la media y desviacion tipica de las
ultimas N lecturas de ESE MISMO sensor (usando el historial ya guardado en
bronze) y la marca como anomalia si se aleja demasiado de esa media
(z-score). El resultado se guarda en una tabla "silver".

Corre EN PARALELO al script de escritura a bronze (read_kafka_to_bronze.py)
-- es un segundo consumidor independiente del mismo topic de Kafka. Los dos
deben estar corriendo a la vez: este script necesita que bronze se siga
alimentando para tener historial fresco contra el que comparar.

Ejecutar dentro del contenedor spark-iceberg (todo en una sola linea, en
una TERCERA terminal -- deja el generador y el escritor de bronze corriendo):

    docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/detectar_anomalias.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

VENTANA_LECTURAS = 20   # cuantas lecturas historicas usar para media/desviacion
Z_SCORE_UMBRAL = 3.0    # a partir de cuantas desviaciones se considera anomalia

spark = SparkSession.builder.appName("DetectarAnomalias").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.silver.lecturas_validadas (
        id_dispositivo STRING,
        tipo_sensor STRING,
        ubicacion STRING,
        valor DOUBLE,
        unidad STRING,
        fecha_actualizacion STRING,
        media_historica DOUBLE,
        desviacion_historica DOUBLE,
        z_score DOUBLE,
        es_anomalia BOOLEAN,
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
        col("evento.payload.after.fecha_actualizacion"),
    )
    .filter(col("id_dispositivo").isNotNull())
)


def procesar_lote(df_lote, id_lote):
    if df_lote.rdd.isEmpty():
        return

    # Importante: usamos la sesión asociada a este micro-lote (no la
    # variable "spark" de fuera), porque foreachBatch entrega el DataFrame
    # en una sesión ligeramente distinta. Si registramos la vista temporal
    # en una sesión y consultamos desde otra, Spark no la encuentra.
    session = df_lote.sparkSession
    df_lote.createOrReplaceTempView("lote_actual")

    resultado = session.sql(f"""
        WITH historico AS (
            SELECT id_dispositivo, AVG(valor) AS media_historica, STDDEV_SAMP(valor) AS desviacion_historica
            FROM (
                SELECT
                    id_dispositivo,
                    valor,
                    ROW_NUMBER() OVER (PARTITION BY id_dispositivo ORDER BY fecha_ingesta DESC) AS rn
                FROM demo.bronze.eventos_dispositivo
            )
            WHERE rn <= {VENTANA_LECTURAS}
            GROUP BY id_dispositivo
        )
        SELECT
            l.id_dispositivo,
            l.tipo_sensor,
            l.ubicacion,
            l.valor,
            l.unidad,
            l.fecha_actualizacion,
            h.media_historica,
            h.desviacion_historica,
            CASE
                WHEN h.desviacion_historica IS NULL OR h.desviacion_historica = 0 THEN 0.0
                ELSE ABS(l.valor - h.media_historica) / h.desviacion_historica
            END AS z_score,
            CASE
                WHEN h.desviacion_historica IS NULL OR h.desviacion_historica = 0 THEN false
                ELSE ABS(l.valor - h.media_historica) / h.desviacion_historica > {Z_SCORE_UMBRAL}
            END AS es_anomalia,
            current_timestamp() AS fecha_ingesta
        FROM lote_actual l
        LEFT JOIN historico h ON l.id_dispositivo = h.id_dispositivo
    """)

    resultado.writeTo("demo.silver.lecturas_validadas").append()

    anomalias = resultado.filter(col("es_anomalia") == True)
    n_anomalias = anomalias.count()
    if n_anomalias > 0:
        print(f"\n--- Lote {id_lote}: {n_anomalias} anomalia(s) detectada(s) ---")
        anomalias.select("id_dispositivo", "tipo_sensor", "valor", "media_historica", "z_score").show(truncate=False)


query = (
    eventos.writeStream
    .outputMode("append")
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/silver_lecturas_validadas")
    .foreachBatch(procesar_lote)
    .start()
)

query.awaitTermination()
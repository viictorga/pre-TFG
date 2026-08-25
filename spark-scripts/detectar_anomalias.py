"""
Fase 3C: Deteccion de fraude en tiempo real por ventana deslizante.

Para cada transaccion nueva, calcula la media y desviacion tipica de los
ultimos N importes de ESA MISMA cuenta (usando el historial ya guardado en
bronze) y la marca como anomalia si se aleja demasiado de esa media
(z-score). El resultado se guarda en demo.silver.transacciones_validadas.

Corre EN PARALELO al escritor de bronze (read_kafka_to_bronze.py) -- ambos
son consumidores independientes del mismo topic de Kafka. Los dos deben
estar corriendo a la vez.

Ejecutar dentro del contenedor spark-iceberg (todo en una sola linea, en
una tercera terminal):

    docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/detectar_anomalias.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

VENTANA_LECTURAS = 20   # cuantas transacciones historicas usar para media/desviacion
Z_SCORE_UMBRAL = 3.0    # a partir de cuantas desviaciones se considera anomalia

spark = SparkSession.builder.appName("DetectarFraude").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.silver.transacciones_validadas (
        id_cuenta STRING,
        tipo_transaccion STRING,
        categoria_comercio STRING,
        canal STRING,
        ubicacion STRING,
        importe DOUBLE,
        moneda STRING,
        resultado STRING,
        fecha_actualizacion STRING,
        media_historica DOUBLE,
        desviacion_historica DOUBLE,
        z_score DOUBLE,
        es_anomalia BOOLEAN,
        fecha_ingesta TIMESTAMP
    ) USING iceberg
""")

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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
        col("evento.payload.after.fecha_actualizacion"),
    )
    .filter(col("id_cuenta").isNotNull())
)


def procesar_lote(df_lote, id_lote):
    if df_lote.rdd.isEmpty():
        return

    # Usamos la sesion asociada a este micro-lote (no una variable "spark"
    # de fuera): foreachBatch entrega el DataFrame en una sesion distinta,
    # y si registramos la vista temporal en una sesion y consultamos desde
    # otra, Spark no la encuentra.
    session = df_lote.sparkSession
    df_lote.createOrReplaceTempView("lote_actual")

    resultado = session.sql(f"""
        WITH historico AS (
            SELECT id_cuenta, AVG(importe) AS media_historica, STDDEV_SAMP(importe) AS desviacion_historica
            FROM (
                SELECT
                    id_cuenta,
                    importe,
                    ROW_NUMBER() OVER (PARTITION BY id_cuenta ORDER BY fecha_ingesta DESC) AS rn
                FROM demo.bronze.eventos_cuenta
            )
            WHERE rn <= {VENTANA_LECTURAS}
            GROUP BY id_cuenta
        )
        SELECT
            l.id_cuenta,
            l.tipo_transaccion,
            l.categoria_comercio,
            l.canal,
            l.ubicacion,
            l.importe,
            l.moneda,
            l.resultado,
            l.fecha_actualizacion,
            h.media_historica,
            h.desviacion_historica,
            CASE
                WHEN h.desviacion_historica IS NULL OR h.desviacion_historica = 0 THEN 0.0
                ELSE ABS(l.importe - h.media_historica) / h.desviacion_historica
            END AS z_score,
            CASE
                WHEN h.desviacion_historica IS NULL OR h.desviacion_historica = 0 THEN false
                ELSE ABS(l.importe - h.media_historica) / h.desviacion_historica > {Z_SCORE_UMBRAL}
            END AS es_anomalia,
            current_timestamp() AS fecha_ingesta
        FROM lote_actual l
        LEFT JOIN historico h ON l.id_cuenta = h.id_cuenta
    """)

    resultado.writeTo("demo.silver.transacciones_validadas").append()

    anomalias = resultado.filter(col("es_anomalia") == True)
    n_anomalias = anomalias.count()
    if n_anomalias > 0:
        print(f"\n--- Lote {id_lote}: {n_anomalias} posible(s) fraude(s) detectado(s) ---")
        anomalias.select("id_cuenta", "tipo_transaccion", "ubicacion", "importe", "media_historica", "z_score").show(truncate=False)


query = (
    eventos.writeStream
    .outputMode("append")
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/silver_transacciones_validadas")
    .foreachBatch(procesar_lote)
    .start()
)

query.awaitTermination()
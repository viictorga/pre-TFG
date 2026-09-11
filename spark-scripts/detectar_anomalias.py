"""
Fase 3C: Deteccion de fraude en tiempo real por ventana deslizante.

Para cada transaccion nueva, calcula la media y desviacion tipica de los
ultimos N importes de ESA MISMA cuenta (usando el historial ya guardado en
bronze) y la marca como anomalia si se aleja demasiado de esa media
(z-score). El resultado se guarda en demo.silver.transacciones_validadas.

La tabla silver arrastra tambien la etiqueta de verdad del generador
(es_anomalia_generada / tipo_anomalia_generada) junto a la prediccion del
detector (es_anomalia). Tener las dos columnas una al lado de la otra es lo
que permite calcular directamente verdaderos positivos, falsos positivos y
falsos negativos con una sola consulta.

La etiqueta se copia, pero NO se usa para decidir: el z-score se calcula solo
a partir del importe y del historico de la cuenta. Si la deteccion mirase la
etiqueta, el experimento no mediria nada.

Corre EN PARALELO al escritor de bronze (read_kafka_to_bronze.py) -- ambos
son consumidores independientes del mismo topic de Kafka. Los dos deben
estar corriendo a la vez.

Ejecutar dentro del contenedor spark-iceberg (todo en una sola linea, en
una tercera terminal):

    docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/detectar_anomalias.py
"""

import os

from metricas import RegistradorDeMetricas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

# --- Parametros de configuracion de la ingesta ---
#
# Los dos determinan juntos la capacidad de absorcion del job, y son las dos
# variables de configuracion mas directamente ligadas a la pregunta de
# investigacion del TFG.
#
# MAX_OFFSETS_POR_TRIGGER acota cuantos eventos entra como maximo en cada
# micro-lote. Sin el, Spark consume en cada disparo TODO lo que haya
# disponible en Kafka, con dos consecuencias: un pico de carga se traga entero
# de una vez, y el consumer lag vale cero por construccion (la resta entre el
# ultimo offset disponible y el ultimo leido siempre da cero). Al acotarlo, lo
# que excede se queda esperando y el lag pasa a medir exactamente eso.
#
# INTERVALO_TRIGGER_SEGUNDOS es cada cuanto se dispara un micro-lote. Junto al
# limite anterior fija el techo teorico de procesamiento del job:
#
#     eventos/s = MAX_OFFSETS_POR_TRIGGER / INTERVALO_TRIGGER_SEGUNDOS
#
# Sin limite de offsets, ese techo lo marca en realidad lo que Spark tarde en
# procesar el micro-lote.
MAX_OFFSETS_POR_TRIGGER = os.getenv("MAX_OFFSETS_POR_TRIGGER")   # None = sin limite
INTERVALO_TRIGGER_SEGUNDOS = os.getenv("INTERVALO_TRIGGER_SEGUNDOS", "15")


def configurar_lector(lector):
    """Aplica el limite de offsets por micro-lote, si se ha pedido uno."""
    if MAX_OFFSETS_POR_TRIGGER:
        print(f"[config] maxOffsetsPerTrigger = {MAX_OFFSETS_POR_TRIGGER}")
        return lector.option("maxOffsetsPerTrigger", MAX_OFFSETS_POR_TRIGGER)
    print("[config] maxOffsetsPerTrigger sin limite: el consumer lag valdra 0 por construccion")
    return lector


VENTANA_LECTURAS = 20   # cuantas transacciones historicas usar para media/desviacion
Z_SCORE_UMBRAL = 3.0    # a partir de cuantas desviaciones se considera anomalia

spark = SparkSession.builder.appName("DetectarFraude").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Registra una fila de metricas por micro-lote (ver metricas.py).
spark.streams.addListener(RegistradorDeMetricas("deteccion"))

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
        es_anomalia_generada BOOLEAN,
        tipo_anomalia_generada STRING,
        fecha_ingesta TIMESTAMP
    ) USING iceberg
""")


def asegurar_columnas(spark, tabla, columnas):
    """Anade a una tabla Iceberg ya existente las columnas que le falten.

    CREATE TABLE IF NOT EXISTS no toca una tabla que ya existe, asi que una
    tabla creada antes de anadir la etiqueta de verdad se quedaria sin esas
    columnas y el append fallaria. Iceberg permite evolucionar el esquema sin
    reescribir los datos: las filas antiguas devuelven NULL en la nueva.
    """
    existentes = {c.lower() for c in spark.table(tabla).columns}
    for nombre, tipo in columnas:
        if nombre.lower() not in existentes:
            spark.sql(f"ALTER TABLE {tabla} ADD COLUMN {nombre} {tipo}")
            print(f"Esquema evolucionado: {tabla} + {nombre} {tipo}")


asegurar_columnas(spark, "demo.silver.transacciones_validadas", [
    ("es_anomalia_generada", "BOOLEAN"),
    ("tipo_anomalia_generada", "STRING"),
])

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

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
    # Etiqueta de verdad: se lee para arrastrarla a silver, nunca para decidir.
    StructField("es_anomalia_generada", BooleanType()),
    StructField("tipo_anomalia_generada", StringType()),
])

envelope_schema = StructType([
    StructField("payload", StructType([
        StructField("after", after_schema),
        StructField("op", StringType()),
    ])),
])

raw = configurar_lector(
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "fraude.public.estado_cuenta")
    .option("startingOffsets", "earliest")
).load()

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
        col("evento.payload.after.es_anomalia_generada"),
        col("evento.payload.after.tipo_anomalia_generada"),
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
            l.es_anomalia_generada,
            l.tipo_anomalia_generada,
            current_timestamp() AS fecha_ingesta
        FROM lote_actual l
        LEFT JOIN historico h ON l.id_cuenta = h.id_cuenta
    """)

    resultado.writeTo("demo.silver.transacciones_validadas").append()

    anomalias = resultado.filter(col("es_anomalia") == True)
    n_anomalias = anomalias.count()
    if n_anomalias > 0:
        print(f"\n--- Lote {id_lote}: {n_anomalias} posible(s) fraude(s) detectado(s) ---")
        anomalias.select(
            "id_cuenta", "tipo_transaccion", "ubicacion", "importe",
            "media_historica", "z_score", "es_anomalia_generada",
        ).show(truncate=False)


query = (
    eventos.writeStream
    .outputMode("append")
    .trigger(processingTime=f"{INTERVALO_TRIGGER_SEGUNDOS} seconds")
    .option("checkpointLocation", "/home/iceberg/warehouse/_checkpoints/silver_transacciones_validadas")
    .foreachBatch(procesar_lote)
    .start()
)

query.awaitTermination()
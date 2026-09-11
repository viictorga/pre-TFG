"""
Instrumentacion de metricas de los jobs de Spark Structured Streaming.

La pregunta de investigacion del TFG es como afectan la carga y la
configuracion al rendimiento de la plataforma, asi que hace falta medir cada
micro-lote, no solo comprobar que el pipeline funciona.

Spark ya calcula todas esas metricas internamente y las publica en un evento
por micro-lote. Lo unico que faltaba era escucharlas y dejarlas por escrito.
Esto es un StreamingQueryListener: Spark lo llama solo cada vez que termina un
micro-lote, sin tocar la logica del pipeline ni anadirle coste apreciable.

De cada micro-lote se guardan dos cosas en el directorio de metricas:

  <job>_<marca-de-tiempo>.csv     columnas planas, listas para analizar
  <job>_<marca-de-tiempo>.jsonl   el progreso completo tal cual lo da Spark

El JSONL existe porque el CSV fija hoy unas columnas concretas y mas adelante
puede hacer falta una metrica que ahora no se esta mirando. Guardar el evento
integro evita tener que repetir un experimento entero por ese motivo.

Uso desde un job:

    from metricas import RegistradorDeMetricas

    spark.streams.addListener(RegistradorDeMetricas("bronze"))

El directorio de salida se controla con la variable de entorno RUTA_METRICAS
y por defecto es /home/iceberg/warehouse/_metricas, que esta montado en
./warehouse/_metricas del repositorio: las metricas quedan accesibles desde
la maquina anfitriona sin copiar nada del contenedor.
"""

import csv
import json
import os
from datetime import datetime, timezone

from pyspark.sql.streaming.listener import StreamingQueryListener

RUTA_METRICAS = os.getenv("RUTA_METRICAS", "/home/iceberg/warehouse/_metricas")

# Columnas del CSV. El orden importa: es el que se escribe en la cabecera.
COLUMNAS = [
    "job",                      # nombre del job que emite la metrica
    "batch_id",                 # numero de micro-lote
    "timestamp",                # instante en que Spark cerro el micro-lote (UTC)
    "filas_entrada",            # eventos procesados en este micro-lote
    "filas_por_segundo_entrada",    # ritmo al que llegan los eventos
    "filas_por_segundo_procesadas",  # ritmo al que el job los procesa
    "duracion_total_ms",        # triggerExecution: cuanto tardo el micro-lote entero
    "duracion_escritura_ms",    # addBatch: cuanto tardo en escribir el resultado
    "duracion_planificacion_ms",    # queryPlanning
    "lag_maximo",               # maxOffsetsBehindLatest: consumer lag de Kafka
    "lag_medio",                # avgOffsetsBehindLatest
]


def _ms(duraciones, clave):
    """Devuelve una duracion del bloque durationMs, o cadena vacia si no esta."""
    valor = duraciones.get(clave)
    return "" if valor is None else valor


class RegistradorDeMetricas(StreamingQueryListener):
    """Escribe una fila por micro-lote con las metricas de rendimiento."""

    def __init__(self, nombre_job, ruta=None):
        self.nombre_job = nombre_job
        self.ruta = ruta or RUTA_METRICAS
        os.makedirs(self.ruta, exist_ok=True)

        marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base = os.path.join(self.ruta, f"{nombre_job}_{marca}")
        self.ruta_csv = base + ".csv"
        self.ruta_jsonl = base + ".jsonl"

        with open(self.ruta_csv, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLUMNAS)

        print(f"[metricas] {self.nombre_job}: escribiendo en {self.ruta_csv}")

    def onQueryStarted(self, event):
        print(f"[metricas] {self.nombre_job}: consulta iniciada, id {event.id}")

    def onQueryProgress(self, event):
        # Se parsea el JSON en vez de leer atributos del objeto porque el JSON
        # es estable entre versiones de Spark y los atributos de Python no.
        progreso = json.loads(event.progress.json)

        with open(self.ruta_jsonl, "a", encoding="utf-8") as f:
            f.write(json.dumps(progreso) + "\n")

        duraciones = progreso.get("durationMs", {})
        fuentes = progreso.get("sources") or [{}]
        # El consumer lag lo publica la propia fuente de Kafka. Es el numero de
        # eventos que ya estan en el topico y el job todavia no ha leido: la
        # medida directa de si el procesamiento va por detras de la ingesta.
        metricas_fuente = fuentes[0].get("metrics") or {}

        fila = [
            self.nombre_job,
            progreso.get("batchId", ""),
            progreso.get("timestamp", ""),
            progreso.get("numInputRows", ""),
            progreso.get("inputRowsPerSecond", ""),
            progreso.get("processedRowsPerSecond", ""),
            _ms(duraciones, "triggerExecution"),
            _ms(duraciones, "addBatch"),
            _ms(duraciones, "queryPlanning"),
            metricas_fuente.get("maxOffsetsBehindLatest", ""),
            metricas_fuente.get("avgOffsetsBehindLatest", ""),
        ]

        with open(self.ruta_csv, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(fila)

    def onQueryIdle(self, event):
        # Spark 3.5 exige implementar este metodo aunque no se use.
        pass

    def onQueryTerminated(self, event):
        print(f"[metricas] {self.nombre_job}: consulta terminada. CSV en {self.ruta_csv}")

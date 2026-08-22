"""
Generador de dispositivos IoT sintéticos.

Simula una red de sensores (temperatura, humedad, movimiento) que actualizan
periódicamente su fila en la tabla "estado_dispositivo" (patrón device shadow).

Cada actualización es justo lo que Debezium capturará como evento CDC más
adelante, así que cuantas más actualizaciones, mejor material para el pipeline.

Ya lleva integrado un parámetro de probabilidad de anomalía (PROB_ANOMALIA)
para cuando llegue el momento de trabajar en la detección de anomalías: por
ahora simplemente genera valores fuera de rango marcados en el log, sin que
nada aguas abajo los procese todavía.
"""

import os
import random
import time
from datetime import datetime, timezone

import psycopg2
from faker import Faker

fake = Faker("es_ES")

DB_CONFIG = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    dbname=os.getenv("DB_NAME", "iot"),
    user=os.getenv("DB_USER", "tfg"),
    password=os.getenv("DB_PASSWORD", "tfg_pass"),
)

NUM_DISPOSITIVOS = int(os.getenv("NUM_DISPOSITIVOS", 30))
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", 2))
PROB_ANOMALIA = float(os.getenv("PROB_ANOMALIA", 0.04))  # 4% de las lecturas

TIPOS_SENSOR = {
    "temperatura": {"unidad": "C", "normal": (15, 30), "anomalo": (-10, 60)},
    "humedad": {"unidad": "%", "normal": (30, 70), "anomalo": (0, 100)},
    "movimiento": {"unidad": "bin", "normal": (0, 1), "anomalo": (0, 1)},
}

UBICACIONES = ["Almacen A", "Almacen B", "Oficina", "Exterior", "Sala servidores"]


def crear_dispositivos():
    dispositivos = []
    for i in range(1, NUM_DISPOSITIVOS + 1):
        tipo = random.choice(list(TIPOS_SENSOR.keys()))
        dispositivos.append({
            "id_dispositivo": f"sensor-{i:03d}",
            "tipo_sensor": tipo,
            "ubicacion": random.choice(UBICACIONES),
        })
    return dispositivos


def generar_lectura(tipo_sensor, anomalo):
    conf = TIPOS_SENSOR[tipo_sensor]
    if tipo_sensor == "movimiento":
        return random.choice([0, 1]), conf["unidad"]
    rango = conf["anomalo"] if anomalo else conf["normal"]
    return round(random.uniform(*rango), 2), conf["unidad"]


def upsert_lectura(cur, dispositivo, valor, unidad):
    cur.execute(
        """
        INSERT INTO estado_dispositivo
            (id_dispositivo, tipo_sensor, ubicacion, valor, unidad, estado, fecha_actualizacion)
        VALUES (%s, %s, %s, %s, %s, 'activo', %s)
        ON CONFLICT (id_dispositivo) DO UPDATE SET
            valor = EXCLUDED.valor,
            estado = 'activo',
            fecha_actualizacion = EXCLUDED.fecha_actualizacion
        """,
        (
            dispositivo["id_dispositivo"],
            dispositivo["tipo_sensor"],
            dispositivo["ubicacion"],
            valor,
            unidad,
            datetime.now(timezone.utc),
        ),
    )


def main():
    dispositivos = crear_dispositivos()
    print(f"Simulando {len(dispositivos)} dispositivos IoT cada {INTERVALO_SEGUNDOS}s. Ctrl+C para parar.\n")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            while True:
                dispositivo = random.choice(dispositivos)
                anomalo = random.random() < PROB_ANOMALIA
                valor, unidad = generar_lectura(dispositivo["tipo_sensor"], anomalo)
                upsert_lectura(cur, dispositivo, valor, unidad)
                marca = "  <-- ANOMALIA" if anomalo else ""
                print(f"{dispositivo['id_dispositivo']:12s} {dispositivo['tipo_sensor']:12s} {valor}{unidad}{marca}")
                time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("\nGenerador detenido.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

"""
Generador de transacciones bancarias sinteticas.

Simula ~30 cuentas que actualizan su fila en "estado_cuenta" (patron
device shadow) cada vez que hacen una nueva transaccion. Cada actualizacion
es justo lo que Debezium capturara como evento CDC.

Ya lleva integrado un parametro de probabilidad de anomalia (PROB_ANOMALIA):
genera transacciones con importes fuera de rango y, la mitad de las veces,
tambien desde una ubicacion distinta a la habitual de esa cuenta (para mas
adelante poder detectar "viajes imposibles").

Detalle importante: categoria_comercio solo tiene sentido en compras
(online o presenciales). En una retirada de cajero o una transferencia no
hay "comercio", asi que ese campo se manda como NULL -- no como texto
vacio ni como un valor inventado tipo "n/a".
.\venv\Scripts\Activate.ps1
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

NUM_CUENTAS = int(os.getenv("NUM_CUENTAS", 30))
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", 2))
PROB_ANOMALIA = float(os.getenv("PROB_ANOMALIA", 0.04))   # 4% de las transacciones
PROB_RECHAZO = float(os.getenv("PROB_RECHAZO", 0.05))     # 5% de las transacciones

# Cada tipo de transaccion define: su canal habitual, si le aplica una
# categoria de comercio, y el rango de importe normal / anomalo.
TIPOS_TRANSACCION = {
    "compra_online": {
        "canal": "online",
        "tiene_categoria": True,
        "normal": (5, 150),
        "anomalo": (500, 3000),
    },
    "compra_presencial": {
        "canal": "presencial",
        "tiene_categoria": True,
        "normal": (5, 100),
        "anomalo": (300, 2000),
    },
    "retirada_cajero": {
        "canal": "cajero",
        "tiene_categoria": False,
        "normal": (20, 300),
        "anomalo": (500, 1000),
    },
    "transferencia": {
        "canal": "app_movil",
        "tiene_categoria": False,
        "normal": (10, 500),
        "anomalo": (1000, 5000),
    },
}

CATEGORIAS_COMERCIO = [
    "supermercado", "electronica", "joyeria", "gasolinera",
    "apuestas", "restauracion", "viajes", "ropa",
]

CIUDADES_HABITUALES = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao"]
CIUDADES_ANOMALAS = ["Bangkok", "Lagos", "Moscu", "Ciudad de Mexico", "Manila"]


def crear_cuentas():
    # Determinista a proposito, igual que con los sensores: la misma
    # cuenta siempre tiene la misma ciudad habitual, incluso si el script
    # se reinicia.
    cuentas = []
    for i in range(1, NUM_CUENTAS + 1):
        cuentas.append({
            "id_cuenta": f"cuenta-{i:03d}",
            "ubicacion_habitual": CIUDADES_HABITUALES[(i - 1) % len(CIUDADES_HABITUALES)],
        })
    return cuentas


def generar_transaccion(cuenta, anomalo):
    tipo_transaccion = random.choice(list(TIPOS_TRANSACCION.keys()))
    conf = TIPOS_TRANSACCION[tipo_transaccion]

    rango = conf["anomalo"] if anomalo else conf["normal"]
    importe = round(random.uniform(*rango), 2)

    # Solo las compras tienen categoria de comercio. None -> NULL en Postgres.
    categoria_comercio = random.choice(CATEGORIAS_COMERCIO) if conf["tiene_categoria"] else None

    # En una transaccion anomala, la mitad de las veces tambien salta la
    # ubicacion habitual de la cuenta (simulando un "viaje imposible").
    if anomalo and random.random() < 0.5:
        ubicacion = random.choice(CIUDADES_ANOMALAS)
    else:
        ubicacion = cuenta["ubicacion_habitual"]

    resultado = "rechazada" if random.random() < PROB_RECHAZO else "aprobada"

    return {
        "tipo_transaccion": tipo_transaccion,
        "categoria_comercio": categoria_comercio,
        "canal": conf["canal"],
        "ubicacion": ubicacion,
        "importe": importe,
        "resultado": resultado,
    }


def upsert_transaccion(cur, cuenta, t):
    cur.execute(
        """
        INSERT INTO estado_cuenta
            (id_cuenta, tipo_transaccion, categoria_comercio, canal, ubicacion, importe, moneda, resultado, estado, fecha_actualizacion)
        VALUES (%s, %s, %s, %s, %s, %s, 'EUR', %s, 'activa', %s)
        ON CONFLICT (id_cuenta) DO UPDATE SET
            tipo_transaccion = EXCLUDED.tipo_transaccion,
            categoria_comercio = EXCLUDED.categoria_comercio,
            canal = EXCLUDED.canal,
            ubicacion = EXCLUDED.ubicacion,
            importe = EXCLUDED.importe,
            moneda = EXCLUDED.moneda,
            resultado = EXCLUDED.resultado,
            estado = 'activa',
            fecha_actualizacion = EXCLUDED.fecha_actualizacion
        """,
        (
            cuenta["id_cuenta"],
            t["tipo_transaccion"],
            t["categoria_comercio"],
            t["canal"],
            t["ubicacion"],
            t["importe"],
            t["resultado"],
            datetime.now(timezone.utc),
        ),
    )


def main():
    cuentas = crear_cuentas()
    print(f"Simulando {len(cuentas)} cuentas bancarias cada {INTERVALO_SEGUNDOS}s. Ctrl+C para parar.\n")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            while True:
                cuenta = random.choice(cuentas)
                anomalo = random.random() < PROB_ANOMALIA
                t = generar_transaccion(cuenta, anomalo)
                upsert_transaccion(cur, cuenta, t)

                categoria_mostrar = t["categoria_comercio"] or "-"
                marca = "  <-- ANOMALIA" if anomalo else ""
                marca += "  [RECHAZADA]" if t["resultado"] == "rechazada" else ""
                print(
                    f"{cuenta['id_cuenta']:12s} {t['tipo_transaccion']:18s} "
                    f"{categoria_mostrar:14s} {t['ubicacion']:18s} {t['importe']:>8.2f}EUR{marca}"
                )
                time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("\nGenerador detenido.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
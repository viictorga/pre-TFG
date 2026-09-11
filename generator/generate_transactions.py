"""
Generador de transacciones bancarias sinteticas.

Simula ~30 cuentas que actualizan su fila en "estado_cuenta" (patron
account shadow) cada vez que hacen una nueva transaccion. Cada actualizacion
es justo lo que Debezium capturara como evento CDC.

Ya lleva integrado un parametro de probabilidad de anomalia (PROB_ANOMALIA):
genera transacciones con importes fuera de rango y, la mitad de las veces,
tambien desde una ubicacion distinta a la habitual de esa cuenta (para mas
adelante poder detectar "viajes imposibles").

Cada transaccion se escribe acompanada de su ETIQUETA DE VERDAD (columnas
es_anomalia_generada y tipo_anomalia_generada): el generador deja constancia
de que inyecto a proposito como anomalo. Esa etiqueta es lo que permite
calcular despues precision y exhaustividad, comparando lo que el pipeline
detecto frente a lo que realmente se genero.

La etiqueta viaja por el pipeline pero NINGUNA logica de deteccion puede
leerla: el detector solo ve importe, cuenta y ubicacion, igual que ocurriria
en un sistema real donde nadie sabe de antemano que transaccion es fraude.
Solo se usa al final, al evaluar los resultados.

Detalle importante: categoria_comercio solo tiene sentido en compras
(online o presenciales). En una retirada de cajero o una transferencia no
hay "comercio", asi que ese campo se manda como NULL -- no como texto
vacio ni como un valor inventado tipo "n/a".


MODO DE PICO DE CARGA
---------------------

Con PICO_ACTIVO=true, el generador alterna su ritmo normal con rafagas de
transacciones muy por encima de la frecuencia habitual. La rafaga simula un
ataque de "card testing": un subconjunto reducido de cuentas emite en pocos
segundos muchos cargos pequenos, que es como se comprueba si una tarjeta
robada sigue activa.

Los picos se planifican antes de empezar y son deterministas: se sabe de
antemano en que segundo empieza y acaba cada uno. Eso es lo que permite
correlacionar despues el consumer lag y la latencia con el momento exacto de
la rafaga, en lugar de tener que deducirlo a ojo.

Las transacciones de una rafaga NO se marcan como anomalas. La anomalia que
el pipeline sabe detectar hoy es de importe, y el card testing se caracteriza
justo por lo contrario: importes pequenos y muchos. Marcarlas como anomalas
falsearia la exhaustividad, midiendo al detector con un patron que nunca fue
disenado para ver. Que exista esa ceguera es un resultado del TFG, no un
error que haya que disimular.


REPRODUCIBILIDAD
----------------

Dos parametros hacen que un experimento se pueda repetir:

  SEMILLA             fija la secuencia aleatoria, de modo que dos
                      ejecuciones con la misma semilla generan exactamente
                      las mismas transacciones. Sin ella, comparar dos
                      configuraciones mezcla el efecto del cambio con el
                      del azar.
  DURACION_SEGUNDOS   corta la ejecucion sola al cabo de N segundos, sin
                      depender de que alguien pulse Ctrl+C en el momento
                      justo.

Al terminar, el generador escribe un resumen en JSON con la configuracion
usada, los contadores de lo generado y las ventanas exactas de los picos.
Ese fichero es la referencia de "eventos generados" contra la que se compara
lo que el pipeline acaba persistiendo, para medir si se perdio algo.
"""

# Recordatorio: activar antes el entorno virtual.
#   PowerShell -> .\venv\Scripts\Activate.ps1
#   bash       -> source venv/bin/activate

import json
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


def _bool_env(nombre, por_defecto=False):
    valor = os.getenv(nombre)
    if valor is None:
        return por_defecto
    return valor.strip().lower() in ("1", "true", "si", "yes", "y")


# --- Carga normal ---
NUM_CUENTAS = int(os.getenv("NUM_CUENTAS", 30))
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", 2))
PROB_ANOMALIA = float(os.getenv("PROB_ANOMALIA", 0.04))   # 4% de las transacciones
PROB_RECHAZO = float(os.getenv("PROB_RECHAZO", 0.05))     # 5% de las transacciones

# --- Control del experimento ---
DURACION_SEGUNDOS = float(os.getenv("DURACION_SEGUNDOS", 0))   # 0 = hasta Ctrl+C
SEMILLA = os.getenv("SEMILLA")                                 # None = no determinista
RUTA_METRICAS = os.getenv("RUTA_METRICAS", os.path.join("warehouse", "_metricas"))
ETIQUETA = os.getenv("ETIQUETA", "generador")   # prefijo del fichero de resumen

# --- Modo de pico de carga ---
PICO_ACTIVO = _bool_env("PICO_ACTIVO", False)
PICO_ESPERA_SEGUNDOS = float(os.getenv("PICO_ESPERA_SEGUNDOS", 60))   # antes del 1er pico y entre picos
PICO_DURACION_SEGUNDOS = float(os.getenv("PICO_DURACION_SEGUNDOS", 30))
PICO_INTERVALO_SEGUNDOS = float(os.getenv("PICO_INTERVALO_SEGUNDOS", 0.01))  # 0 = lo mas rapido posible
PICO_REPETICIONES = int(os.getenv("PICO_REPETICIONES", 1))
PICO_NUM_CUENTAS = int(os.getenv("PICO_NUM_CUENTAS", 5))      # cuantas cuentas participan en la rafaga
PICO_IMPORTE_MAX = float(os.getenv("PICO_IMPORTE_MAX", 3.0))  # cargos pequenos, propios del card testing

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
    # Determinista a proposito: la misma cuenta siempre tiene la misma
    # ciudad habitual, incluso si el script se reinicia. Asi el "viaje
    # imposible" de una anomalia es reproducible entre experimentos.
    cuentas = []
    for i in range(1, NUM_CUENTAS + 1):
        cuentas.append({
            "id_cuenta": f"cuenta-{i:03d}",
            "ubicacion_habitual": CIUDADES_HABITUALES[(i - 1) % len(CIUDADES_HABITUALES)],
        })
    return cuentas


def planificar_picos():
    """Calcula cuando empieza y acaba cada pico, en segundos desde el arranque.

    Se planifica de antemano y no sobre la marcha para que las ventanas sean
    conocidas antes de empezar: al analizar las metricas hace falta saber con
    precision en que intervalo estuvo el sistema bajo rafaga.
    """
    if not PICO_ACTIVO or PICO_REPETICIONES <= 0:
        return []

    ventanas = []
    instante = PICO_ESPERA_SEGUNDOS
    for _ in range(PICO_REPETICIONES):
        ventanas.append((instante, instante + PICO_DURACION_SEGUNDOS))
        instante += PICO_DURACION_SEGUNDOS + PICO_ESPERA_SEGUNDOS
    return ventanas


def en_ventana(transcurrido, ventanas):
    """Indica si el instante dado cae dentro de alguna ventana de pico."""
    return any(inicio <= transcurrido < fin for inicio, fin in ventanas)


def generar_transaccion(cuenta, anomalo):
    tipo_transaccion = random.choice(list(TIPOS_TRANSACCION.keys()))
    conf = TIPOS_TRANSACCION[tipo_transaccion]

    rango = conf["anomalo"] if anomalo else conf["normal"]
    importe = round(random.uniform(*rango), 2)

    # Solo las compras tienen categoria de comercio. None -> NULL en Postgres.
    categoria_comercio = random.choice(CATEGORIAS_COMERCIO) if conf["tiene_categoria"] else None

    # En una transaccion anomala, la mitad de las veces tambien salta la
    # ubicacion habitual de la cuenta (simulando un "viaje imposible").
    salto_ubicacion = anomalo and random.random() < 0.5
    if salto_ubicacion:
        ubicacion = random.choice(CIUDADES_ANOMALAS)
    else:
        ubicacion = cuenta["ubicacion_habitual"]

    resultado = "rechazada" if random.random() < PROB_RECHAZO else "aprobada"

    # Etiqueta de verdad: distinguimos los dos tipos de anomalia inyectada
    # para poder medir por separado cuanto detecta el pipeline de cada uno.
    if not anomalo:
        tipo_anomalia = None
    elif salto_ubicacion:
        tipo_anomalia = "importe_ubicacion"
    else:
        tipo_anomalia = "importe"

    return {
        "tipo_transaccion": tipo_transaccion,
        "categoria_comercio": categoria_comercio,
        "canal": conf["canal"],
        "ubicacion": ubicacion,
        "importe": importe,
        "resultado": resultado,
        "es_anomalia_generada": anomalo,
        "tipo_anomalia_generada": tipo_anomalia,
    }


def generar_transaccion_pico(cuenta):
    """Transaccion de una rafaga de card testing: compra online de importe minimo.

    Se queda en la ubicacion habitual de la cuenta y no se marca como anomala:
    lo sospechoso del card testing es el ritmo, no el importe ni el sitio.
    Cada transaccion por separado es indistinguible de una compra pequena
    normal, que es justo lo que la hace dificil de detectar con un umbral
    sobre el importe.
    """
    return {
        "tipo_transaccion": "compra_online",
        "categoria_comercio": random.choice(CATEGORIAS_COMERCIO),
        "canal": "online",
        "ubicacion": cuenta["ubicacion_habitual"],
        "importe": round(random.uniform(0.5, PICO_IMPORTE_MAX), 2),
        "resultado": "rechazada" if random.random() < 0.5 else "aprobada",
        "es_anomalia_generada": False,
        "tipo_anomalia_generada": None,
    }


def upsert_transaccion(cur, cuenta, t):
    cur.execute(
        """
        INSERT INTO estado_cuenta
            (id_cuenta, tipo_transaccion, categoria_comercio, canal, ubicacion, importe, moneda, resultado, estado, fecha_actualizacion, es_anomalia_generada, tipo_anomalia_generada)
        VALUES (%s, %s, %s, %s, %s, %s, 'EUR', %s, 'activa', %s, %s, %s)
        ON CONFLICT (id_cuenta) DO UPDATE SET
            tipo_transaccion = EXCLUDED.tipo_transaccion,
            categoria_comercio = EXCLUDED.categoria_comercio,
            canal = EXCLUDED.canal,
            ubicacion = EXCLUDED.ubicacion,
            importe = EXCLUDED.importe,
            moneda = EXCLUDED.moneda,
            resultado = EXCLUDED.resultado,
            estado = 'activa',
            fecha_actualizacion = EXCLUDED.fecha_actualizacion,
            es_anomalia_generada = EXCLUDED.es_anomalia_generada,
            tipo_anomalia_generada = EXCLUDED.tipo_anomalia_generada
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
            t["es_anomalia_generada"],
            t["tipo_anomalia_generada"],
        ),
    )


def escribir_resumen(contadores, ventanas_absolutas, inicio_iso, fin_iso, duracion_real):
    """Deja por escrito que se genero exactamente, para poder evaluarlo despues.

    Con el patron account shadow, PostgreSQL solo conserva la ultima
    transaccion de cada cuenta, asi que el numero de eventos generados no se
    puede reconstruir despues consultando la base de datos: hay que anotarlo
    aqui. Es la referencia contra la que se compara lo que el pipeline acaba
    persistiendo, para saber si se perdio algo por el camino.
    """
    os.makedirs(RUTA_METRICAS, exist_ok=True)
    marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ruta = os.path.join(RUTA_METRICAS, f"{ETIQUETA}_{marca}.json")

    resumen = {
        "inicio": inicio_iso,
        "fin": fin_iso,
        "duracion_real_s": round(duracion_real, 2),
        "configuracion": {
            "semilla": SEMILLA,
            "num_cuentas": NUM_CUENTAS,
            "intervalo_segundos": INTERVALO_SEGUNDOS,
            "prob_anomalia": PROB_ANOMALIA,
            "prob_rechazo": PROB_RECHAZO,
            "duracion_segundos": DURACION_SEGUNDOS,
            "pico_activo": PICO_ACTIVO,
            "pico_espera_segundos": PICO_ESPERA_SEGUNDOS,
            "pico_duracion_segundos": PICO_DURACION_SEGUNDOS,
            "pico_intervalo_segundos": PICO_INTERVALO_SEGUNDOS,
            "pico_repeticiones": PICO_REPETICIONES,
            "pico_num_cuentas": PICO_NUM_CUENTAS,
            "pico_importe_max": PICO_IMPORTE_MAX,
        },
        "contadores": contadores,
        "ritmo_medio_eventos_s": round(contadores["total"] / duracion_real, 2) if duracion_real > 0 else 0,
        "ventanas_pico": ventanas_absolutas,
    }

    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)

    return ruta, resumen


def main():
    if SEMILLA is not None:
        random.seed(SEMILLA)

    cuentas = crear_cuentas()
    ventanas = planificar_picos()

    # El card testing ataca unas pocas cuentas, no todas: es lo que concentra
    # la carga y hace que la rafaga se note tambien en el reparto por clave
    # de las particiones de Kafka.
    cuentas_pico = cuentas[: max(1, min(PICO_NUM_CUENTAS, len(cuentas)))]

    print(f"Simulando {len(cuentas)} cuentas bancarias cada {INTERVALO_SEGUNDOS}s.")
    if SEMILLA is not None:
        print(f"Semilla fijada en {SEMILLA!r}: la ejecucion es reproducible.")
    if DURACION_SEGUNDOS > 0:
        print(f"La ejecucion se detendra sola a los {DURACION_SEGUNDOS:.0f}s.")
    if ventanas:
        print(f"Modo pico ACTIVO: {len(ventanas)} rafaga(s) de {PICO_DURACION_SEGUNDOS:.0f}s "
              f"a un evento cada {PICO_INTERVALO_SEGUNDOS}s sobre {len(cuentas_pico)} cuentas.")
        for i, (ini, fin) in enumerate(ventanas, 1):
            print(f"  pico {i}: de t+{ini:.0f}s a t+{fin:.0f}s")
    print("Ctrl+C para parar.\n")

    contadores = {
        "total": 0,
        "normales": 0,
        "anomalas": 0,
        "anomalas_importe": 0,
        "anomalas_importe_ubicacion": 0,
        "rechazadas": 0,
        "en_pico": 0,
    }
    ventanas_absolutas = []
    pico_anterior = False

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    inicio_mono = time.monotonic()
    inicio_iso = datetime.now(timezone.utc).isoformat()

    try:
        with conn.cursor() as cur:
            while True:
                transcurrido = time.monotonic() - inicio_mono
                if DURACION_SEGUNDOS > 0 and transcurrido >= DURACION_SEGUNDOS:
                    break

                dentro_de_pico = en_ventana(transcurrido, ventanas)

                # Traza de entrada y salida de cada rafaga, para poder marcarla
                # despues sobre las graficas de lag y de latencia.
                if dentro_de_pico and not pico_anterior:
                    ventanas_absolutas.append({"inicio": datetime.now(timezone.utc).isoformat(), "fin": None})
                    print(f"\n>>> PICO DE CARGA: rafaga iniciada en t+{transcurrido:.0f}s <<<\n")
                elif not dentro_de_pico and pico_anterior:
                    ventanas_absolutas[-1]["fin"] = datetime.now(timezone.utc).isoformat()
                    print(f"\n>>> PICO DE CARGA: rafaga terminada en t+{transcurrido:.0f}s "
                          f"({contadores['en_pico']} eventos acumulados en picos) <<<\n")
                pico_anterior = dentro_de_pico

                if dentro_de_pico:
                    cuenta = random.choice(cuentas_pico)
                    t = generar_transaccion_pico(cuenta)
                    intervalo = PICO_INTERVALO_SEGUNDOS
                    contadores["en_pico"] += 1
                else:
                    cuenta = random.choice(cuentas)
                    anomalo = random.random() < PROB_ANOMALIA
                    t = generar_transaccion(cuenta, anomalo)
                    intervalo = INTERVALO_SEGUNDOS

                upsert_transaccion(cur, cuenta, t)

                contadores["total"] += 1
                if t["es_anomalia_generada"]:
                    contadores["anomalas"] += 1
                    contadores["anomalas_" + t["tipo_anomalia_generada"]] += 1
                else:
                    contadores["normales"] += 1
                if t["resultado"] == "rechazada":
                    contadores["rechazadas"] += 1

                # Durante una rafaga no se imprime cada transaccion: a cientos
                # por segundo, escribir en consola pasaria a ser el cuello de
                # botella y falsearia el ritmo que se pretende medir.
                if not dentro_de_pico:
                    categoria_mostrar = t["categoria_comercio"] or "-"
                    marca = f"  <-- ANOMALIA ({t['tipo_anomalia_generada']})" if t["es_anomalia_generada"] else ""
                    marca += "  [RECHAZADA]" if t["resultado"] == "rechazada" else ""
                    print(
                        f"{cuenta['id_cuenta']:12s} {t['tipo_transaccion']:18s} "
                        f"{categoria_mostrar:14s} {t['ubicacion']:18s} {t['importe']:>8.2f}EUR{marca}"
                    )

                if intervalo > 0:
                    time.sleep(intervalo)
    except KeyboardInterrupt:
        print("\nGenerador detenido.")
    finally:
        conn.close()

        duracion_real = time.monotonic() - inicio_mono
        fin_iso = datetime.now(timezone.utc).isoformat()
        if ventanas_absolutas and ventanas_absolutas[-1]["fin"] is None:
            ventanas_absolutas[-1]["fin"] = fin_iso

        ruta, resumen = escribir_resumen(
            contadores, ventanas_absolutas, inicio_iso, fin_iso, duracion_real
        )

        print(f"\n--- Resumen ({duracion_real:.1f}s) ---")
        print(f"Eventos generados : {contadores['total']}")
        print(f"  normales        : {contadores['normales']}")
        print(f"  anomalos        : {contadores['anomalas']} "
              f"(importe {contadores['anomalas_importe']}, "
              f"importe_ubicacion {contadores['anomalas_importe_ubicacion']})")
        print(f"  en picos        : {contadores['en_pico']}")
        print(f"  rechazados      : {contadores['rechazadas']}")
        print(f"Ritmo medio       : {resumen['ritmo_medio_eventos_s']} eventos/s")
        print(f"Resumen escrito en: {ruta}")


if __name__ == "__main__":
    main()

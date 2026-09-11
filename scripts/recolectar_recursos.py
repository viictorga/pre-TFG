"""
Muestreo periodico del consumo de recursos de los contenedores.

El listener de Spark (spark-scripts/metricas.py) mide el rendimiento del
procesamiento: cuantos eventos entran, cuanto tarda cada micro-lote y cuanto
lag acumula Kafka. Lo que no puede ver desde dentro es el coste en CPU y
memoria de cada servicio de la plataforma, que es la otra mitad de la
pregunta de investigacion.

Este script se ejecuta en la maquina anfitriona, no dentro de un contenedor,
porque necesita el CLI de Docker. Muestrea "docker stats" cada N segundos y
escribe una fila por contenedor y muestra en un CSV.

Uso:

    python scripts/recolectar_recursos.py --intervalo 5 --duracion 300

Con --duracion 0 (por defecto) muestrea indefinidamente hasta Ctrl+C, que es
lo comodo cuando se esta calibrando un experimento a mano.

El CSV se escribe por defecto en warehouse/_metricas/, el mismo sitio donde
el listener de Spark deja las suyas, para que todas las medidas de un mismo
experimento queden juntas. Ese directorio esta excluido del control de
versiones.

Precaucion importante para la fase experimental: "docker stats" es en si
mismo un proceso que consume CPU. Con intervalos muy cortos (por debajo de
uno o dos segundos) la propia medicion empieza a contaminar lo medido. Un
intervalo de 5 segundos es un compromiso razonable para experimentos de
varios minutos.
"""

import argparse
import csv
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

RUTA_POR_DEFECTO = os.path.join("warehouse", "_metricas")

FORMATO = "{{.Name}};{{.CPUPerc}};{{.MemUsage}};{{.MemPerc}};{{.NetIO}};{{.BlockIO}}"

COLUMNAS = [
    "timestamp",        # instante de la muestra (UTC)
    "contenedor",
    "cpu_pct",          # porcentaje de CPU; 100 = un nucleo completo
    "mem_usada_mib",
    "mem_limite_mib",
    "mem_pct",
    "red_entrada",
    "red_salida",
    "disco_lectura",
    "disco_escritura",
]

# Factores para pasar a MiB las unidades que usa docker stats.
FACTORES = {"B": 1 / (1024 * 1024), "KIB": 1 / 1024, "MIB": 1, "GIB": 1024, "KB": 1 / 1024, "MB": 1, "GB": 1024}


# Los sufijos se prueban de mas largo a mas corto: "123.4MiB" termina tambien
# en "B", y comprobar "B" primero lo interpretaria como bytes y fallaria.
SUFIJOS = sorted(FACTORES, key=len, reverse=True)


def a_mib(texto):
    """Convierte un valor tipo '1.234GiB' a MiB. Devuelve '' si no se entiende."""
    texto = texto.strip()
    for sufijo in SUFIJOS:
        factor = FACTORES[sufijo]
        if texto.upper().endswith(sufijo):
            try:
                return round(float(texto[: -len(sufijo)]) * factor, 2)
            except ValueError:
                return ""
    return ""


def muestrear():
    """Ejecuta docker stats una vez y devuelve una lista de filas."""
    salida = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", FORMATO],
        capture_output=True,
        text=True,
        check=True,
    ).stdout

    ahora = datetime.now(timezone.utc).isoformat()
    filas = []

    for linea in salida.strip().splitlines():
        if not linea.strip():
            continue
        partes = linea.split(";")
        if len(partes) != 6:
            continue
        nombre, cpu, mem_uso, mem_pct, red, disco = partes

        # "123.4MiB / 7.654GiB" -> usado y limite por separado
        usado, _, limite = mem_uso.partition("/")
        entrada_red, _, salida_red = red.partition("/")
        lectura, _, escritura = disco.partition("/")

        filas.append([
            ahora,
            nombre.strip(),
            cpu.strip().rstrip("%"),
            a_mib(usado),
            a_mib(limite),
            mem_pct.strip().rstrip("%"),
            entrada_red.strip(),
            salida_red.strip(),
            lectura.strip(),
            escritura.strip(),
        ])

    return filas


def main():
    parser = argparse.ArgumentParser(description="Muestrea CPU y memoria de los contenedores.")
    parser.add_argument("--intervalo", type=float, default=5.0, help="segundos entre muestras (por defecto 5)")
    parser.add_argument("--duracion", type=float, default=0, help="segundos totales; 0 = hasta Ctrl+C")
    parser.add_argument("--salida", default=None, help="ruta del CSV; por defecto warehouse/_metricas/recursos_<marca>.csv")
    parser.add_argument("--etiqueta", default="recursos", help="prefijo del nombre del fichero, util para nombrar un experimento")
    args = parser.parse_args()

    if args.salida:
        ruta = args.salida
    else:
        os.makedirs(RUTA_POR_DEFECTO, exist_ok=True)
        marca = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        ruta = os.path.join(RUTA_POR_DEFECTO, f"{args.etiqueta}_{marca}.csv")

    inicio = time.monotonic()
    muestras = 0

    print(f"Muestreando cada {args.intervalo}s -> {ruta}")
    print("Ctrl+C para parar.\n")

    with open(ruta, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(COLUMNAS)

        try:
            while True:
                try:
                    filas = muestrear()
                except subprocess.CalledProcessError as e:
                    print(f"docker stats fallo: {e.stderr.strip()}", file=sys.stderr)
                    break

                escritor.writerows(filas)
                f.flush()   # para poder mirar el CSV mientras el experimento corre
                muestras += 1
                print(f"muestra {muestras}: {len(filas)} contenedores", end="\r")

                if args.duracion and (time.monotonic() - inicio) >= args.duracion:
                    break
                time.sleep(args.intervalo)
        except KeyboardInterrupt:
            pass

    print(f"\n{muestras} muestras escritas en {ruta}")


if __name__ == "__main__":
    main()

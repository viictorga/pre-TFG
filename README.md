# TFG — Plataforma de ingeniería de datos en streaming con CDC

Pipeline de datos de extremo a extremo que captura cambios en tiempo real
mediante *Change Data Capture*, detecta anomalías sobre el flujo de eventos y
los persiste en un *data lakehouse* por capas. Todo se despliega con Docker
Compose en una sola máquina y con herramientas exclusivamente gratuitas y de
código abierto.

**Caso de uso:** detección de fraude en transacciones bancarias simuladas.

El protagonista del proyecto es la plataforma, no el caso de uso: el escenario
bancario sirve para poder medir de forma objetiva cómo se comporta el sistema
ante carga, anomalías y eventos corruptos.

## Arquitectura

```text
generador (Python + Faker)
        │  UPSERT
        ▼
   PostgreSQL · tabla estado_cuenta        (patrón "account shadow")
        │  CDC
        ▼
   Debezium · conector "fraude-connector"
        │
        ▼
   Kafka · topic fraude.public.estado_cuenta
        │
        ├──────────────────────────┐
        ▼                          ▼
 read_kafka_to_bronze.py    detectar_anomalias.py
        │                          │
        ▼                          ▼
 demo.bronze.eventos_cuenta  demo.silver.transacciones_validadas
        (Iceberg sobre MinIO)
```

El patrón *account shadow* implica que `estado_cuenta` guarda **una única fila
por cuenta** con su transacción más reciente: cada nueva operación es un
`UPDATE`, no un `INSERT`. Esto genera muchas más actualizaciones que
inserciones, que es justo el escenario donde el CDC aporta valor, y hace que el
histórico completo viva en la capa `bronze` y no en Postgres.

## Requisitos

- Docker y Docker Compose
- Python 3.9 o superior (solo para el generador, que corre fuera de Docker)

## 1. Levantar el entorno

```bash
docker compose up -d
docker compose ps
```

Deben quedar en marcha siete servicios: `tfg-postgres`, `tfg-zookeeper`,
`tfg-kafka`, `tfg-connect`, `tfg-spark`, `tfg-iceberg-rest` y `tfg-minio`. El
octavo, `tfg-mc`, solo crea el bucket `warehouse` en MinIO al arrancar.

`tfg-connect` es el que más tarda: dale 20-30 segundos antes de registrar el
conector. La base de datos `iot`, el usuario `tfg` y la tabla `estado_cuenta` se
crean solos a partir de `sql/init.sql`, y Postgres arranca ya con
`wal_level=logical`, que es lo que Debezium necesita para el CDC.

> El nombre de la base de datos (`iot`) es herencia del caso de uso anterior del
> proyecto, que simulaba sensores IoT. Se ha mantenido a propósito para no
> recrear el volumen; no afecta al funcionamiento.

## 2. Registrar el conector de Debezium

Comprueba primero que Connect responde:

```bash
curl -s http://localhost:8083/connectors
```

Devuelve `[]` si todavía no hay ningún conector. Regístralo:

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium/register-connector.json
```

Un `201 Created` significa que se ha registrado. Verifica que está corriendo:

```bash
curl -s localhost:8083/connectors/fraude-connector/status
```

Busca `"state":"RUNNING"` tanto en el conector como en su tarea.

## 3. Lanzar el generador de transacciones

```bash
cd generator
python -m venv venv
source venv/bin/activate          # en Windows PowerShell: .\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python generate_transactions.py
```

Salida esperada, una línea por transacción:

```text
cuenta-012   compra_online      electronica    Madrid              43.90EUR
cuenta-004   retirada_cajero    -              Sevilla            120.00EUR
cuenta-021   transferencia      -              Bangkok           3204.55EUR  <-- ANOMALIA
cuenta-007   compra_presencial  supermercado   Bilbao              18.35EUR  [RECHAZADA]
```

Se para con `Ctrl+C`.

### Parámetros del generador

Todos se controlan por variable de entorno:

| Variable | Por defecto | Qué hace |
|---|---|---|
| `NUM_CUENTAS` | 30 | Cuántas cuentas bancarias simular |
| `INTERVALO_SEGUNDOS` | 2 | Segundos entre transacciones |
| `PROB_ANOMALIA` | 0.04 | Probabilidad de generar una transacción con importe anómalo |
| `PROB_RECHAZO` | 0.05 | Probabilidad de que la transacción salga rechazada |
| `DB_HOST` / `DB_PORT` | `localhost` / `5432` | Conexión a Postgres |
| `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `iot` / `tfg` / `tfg_pass` | Credenciales |

Ejemplo con más carga y más anomalías:

```bash
NUM_CUENTAS=10 INTERVALO_SEGUNDOS=0.2 PROB_ANOMALIA=0.2 python generate_transactions.py
```

Una transacción anómala lleva un importe fuera del rango habitual de su tipo y,
la mitad de las veces, también una ubicación distinta a la habitual de esa
cuenta (base para detectar más adelante patrones de "viaje imposible").

Quien decide si una transacción es sospechosa es la capa de streaming, no el
origen: el generador se limita a inyectar el importe fuera de rango. Pero sí
deja constancia de lo que inyectó, en dos columnas de **etiqueta de verdad**:

| Columna | Valores | Para qué |
|---|---|---|
| `es_anomalia_generada` | `true` / `false` | Si el generador inyectó esta transacción como anómala |
| `tipo_anomalia_generada` | `NULL`, `importe`, `importe_ubicacion` | Qué tipo de anomalía inyectó |

Esa etiqueta viaja por todo el pipeline hasta la capa `silver`, donde queda
junto a la predicción del detector (`es_anomalia`). Comparando ambas columnas se
calculan precisión y exhaustividad. **Ninguna lógica de detección lee la
etiqueta**: el z-score se calcula solo con el importe y el histórico de la
cuenta, igual que en un sistema real, donde nadie sabe de antemano qué
transacción es fraude.

## 4. Comprobar que los datos llegan

Estado de las cuentas en Postgres (una fila por cuenta):

```bash
docker exec -it tfg-postgres psql -U tfg -d iot \
  -c "SELECT id_cuenta, tipo_transaccion, importe, ubicacion, es_anomalia_generada FROM estado_cuenta ORDER BY fecha_actualizacion DESC LIMIT 10;"
```

Eventos CDC en Kafka:

```bash
docker exec -it tfg-kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic fraude.public.estado_cuenta \
  --from-beginning
```

> **Importante:** dentro del contenedor hay que usar `kafka:9092`, no
> `localhost:9092` — el broker anuncia su nombre de servicio y con `localhost`
> las herramientas de consola se quedan esperando un nodo que nunca aparece.

> **Usuarios de Git Bash en Windows:** Git Bash reescribe las rutas absolutas
> de los argumentos y rompe los `docker exec` que invocan scripts dentro del
> contenedor (`/kafka/bin/...` acaba convertido en `C:/Program Files/Git/kafka/...`).
> Exporta `MSYS_NO_PATHCONV=1` antes de ejecutarlos, o usa PowerShell.

El topic no existe hasta que llega el primer evento: necesitas el generador
corriendo y el conector en `RUNNING`.

## 5. Procesar el flujo con Spark

Los jobs viven en `spark-scripts/` y se ejecutan dentro de `tfg-spark`. Cada uno
en su propia terminal, porque son procesos de streaming que no terminan:

```bash
# Solo mira los eventos por consola, no escribe nada. Útil para comprobar el CDC.
docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/read_kafka_console.py

# Escribe el histórico completo de transacciones en la capa bronze.
docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/read_kafka_to_bronze.py

# Detecta anomalías por z-score y escribe en la capa silver.
docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/detectar_anomalias.py
```

`read_kafka_to_bronze.py` y `detectar_anomalias.py` son **consumidores
independientes del mismo topic** y están pensados para correr a la vez: el
detector calcula la media y la desviación del importe de cada cuenta leyendo el
histórico que el primero va dejando en `bronze`.

Consultar los resultados:

```bash
docker exec -it tfg-spark spark-sql \
  -e "SELECT count(*) FROM demo.bronze.eventos_cuenta;"

docker exec -it tfg-spark spark-sql \
  -e "SELECT id_cuenta, importe, media_historica, z_score, es_anomalia FROM demo.silver.transacciones_validadas WHERE es_anomalia ORDER BY fecha_ingesta DESC LIMIT 20;"
```

### Evaluar la detección

La capa `silver` guarda una al lado de la otra la predicción del detector
(`es_anomalia`) y la etiqueta de verdad del generador (`es_anomalia_generada`),
así que la matriz de confusión sale de una sola consulta:

```bash
docker exec -it tfg-spark spark-sql -e "
SELECT
  count(*)                                                                      AS total,
  sum(CASE WHEN es_anomalia     AND es_anomalia_generada     THEN 1 ELSE 0 END) AS verdaderos_positivos,
  sum(CASE WHEN es_anomalia     AND NOT es_anomalia_generada THEN 1 ELSE 0 END) AS falsos_positivos,
  sum(CASE WHEN NOT es_anomalia AND es_anomalia_generada     THEN 1 ELSE 0 END) AS falsos_negativos
FROM demo.silver.transacciones_validadas;"
```

Y la exhaustividad desglosada por tipo de anomalía inyectada:

```bash
docker exec -it tfg-spark spark-sql -e "
SELECT tipo_anomalia_generada,
       count(*)                                     AS inyectadas,
       sum(CASE WHEN es_anomalia THEN 1 ELSE 0 END) AS detectadas
FROM demo.silver.transacciones_validadas
WHERE es_anomalia_generada
GROUP BY tipo_anomalia_generada;"
```

La consola web de MinIO está en <http://localhost:9001> (`admin` / `password`) y
la interfaz de Spark en <http://localhost:8080>.

## 6. Medir el rendimiento

La pregunta de investigación del TFG es cómo afectan la carga y la
configuración al rendimiento de la plataforma, así que los dos jobs de
streaming van instrumentados. Todas las medidas se escriben en
`warehouse/_metricas/`, que está fuera del control de versiones.

### Métricas del procesamiento

`spark-scripts/metricas.py` engancha un `StreamingQueryListener` a los jobs:
Spark ya calcula estas métricas internamente, el listener solo las escucha y
las deja por escrito, sin tocar la lógica del pipeline. No hay que hacer nada
para activarlo, ya viene registrado en los dos jobs.

Por cada micro-lote se escriben dos ficheros:

| Fichero | Contenido |
|---|---|
| `<job>_<marca>.csv` | Columnas planas listas para analizar |
| `<job>_<marca>.jsonl` | El progreso completo tal cual lo publica Spark |

El JSONL se guarda porque el CSV fija hoy unas columnas concretas, y así una
métrica que ahora no se está mirando no obliga a repetir el experimento
entero. Columnas del CSV:

| Columna | Qué mide |
|---|---|
| `filas_entrada` | Eventos procesados en el micro-lote |
| `filas_por_segundo_entrada` | Ritmo al que llegan los eventos |
| `filas_por_segundo_procesadas` | **Throughput** del job |
| `duracion_total_ms` | Cuánto tardó el micro-lote entero |
| `duracion_escritura_ms` | Cuánto se fue en escribir en Iceberg |
| `lag_maximo` / `lag_medio` | **Consumer lag**: eventos ya en Kafka que el job aún no ha leído |

El `lag` es la señal clave en las pruebas de pico de carga: mientras se
mantenga cerca de cero, el job va al día; si empieza a crecer, el
procesamiento se está quedando por detrás de la ingesta.

### Consumo de CPU y memoria

El listener no puede ver desde dentro lo que cuesta cada servicio. Eso lo
muestrea `scripts/recolectar_recursos.py`, que corre en el anfitrión (necesita
el CLI de Docker) y lanza `docker stats` periódicamente:

```bash
python scripts/recolectar_recursos.py --intervalo 5 --duracion 300 --etiqueta pico_carga
```

Con `--duracion 0` muestrea hasta `Ctrl+C`. La etiqueta da nombre al fichero,
lo que resulta cómodo para identificar cada experimento.

> `docker stats` también consume CPU. Por debajo de uno o dos segundos de
> intervalo la propia medición empieza a contaminar lo medido; 5 segundos es un
> compromiso razonable para experimentos de varios minutos.

### Latencia de extremo a extremo

La capa `bronze` guarda el instante en que el generador escribió en Postgres
(`fecha_actualizacion`) y el instante en que Spark persistió el evento
(`fecha_ingesta`), así que la latencia del pipeline completo sale de una
consulta:

```bash
docker exec -it tfg-spark spark-sql -e "
SELECT
  round(avg(unix_timestamp(fecha_ingesta) - unix_timestamp(to_timestamp(fecha_actualizacion))), 2) AS latencia_media_s,
  round(max(unix_timestamp(fecha_ingesta) - unix_timestamp(to_timestamp(fecha_actualizacion))), 2) AS latencia_maxima_s,
  count(*) AS eventos
FROM demo.bronze.eventos_cuenta;"
```

> Esa latencia incluye el tiempo de espera del *trigger* del job, que hoy es de
> 10 segundos en `bronze`: un evento que llega justo después de un micro-lote
> espera hasta el siguiente. Es decir, el grueso de la latencia medida es una
> decisión de configuración, no un límite del sistema — y precisamente por eso
> el tamaño del trigger es uno de los parámetros interesantes que barrer en los
> experimentos.

> `fecha_actualizacion` la pone el generador (que corre en el anfitrión) y
> `fecha_ingesta` la pone Spark (dentro de un contenedor). Si los relojes del
> anfitrión y de los contenedores se desincronizan, la latencia medida se
> desplaza. Conviene comprobarlo antes de dar por buenos los números.

## 7. Parar el entorno

```bash
docker compose down       # para los contenedores y conserva el volumen de Postgres
```

> **No ejecutes `docker compose down -v`** salvo que quieras borrar también el
> volumen de Postgres. Y ten en cuenta que **MinIO no tiene volumen propio en el
> `docker-compose.yml` actual**: los datos de Iceberg viven dentro del
> contenedor, así que un `docker compose down` normal ya se los lleva por
> delante. Si necesitas conservar el lakehouse entre sesiones, usa
> `docker compose stop` o añade un volumen a MinIO.

## Estructura del repositorio

```text
docker-compose.yml            # los 8 servicios de la plataforma
sql/init.sql                  # esquema de la tabla estado_cuenta
generator/                    # generador de transacciones sintéticas
debezium/                     # configuración del conector CDC
spark-scripts/                # jobs de Spark Structured Streaming + instrumentación
scripts/                      # utilidades que corren en el anfitrión
documentation/                # propuesta del TFG
warehouse/                    # datos de Iceberg y métricas (ignorado por git)
```

`CLAUDE.md` contiene la memoria completa del proyecto: objetivos, decisiones
tomadas y su justificación, plan por fases y estado real del código.

## Estado del proyecto

Funcionando: generador, CDC con Debezium, ingesta en Kafka, escritura en la capa
`bronze` y detección de anomalías por z-score hacia `silver`.

La etiqueta de verdad ya permite calcular precisión y exhaustividad de la
detección sobre la capa `silver`, y los jobs están instrumentados para medir
throughput, latencia, consumer lag y consumo de recursos.

Pendiente: modos de pico de carga y de eventos corruptos en el generador, cola
de eventos fallidos (DLQ), capa `gold`, transformaciones con dbt, orquestación
con Airflow, consulta con DuckDB, panel de visualización e instrumentación de
métricas para la evaluación experimental.

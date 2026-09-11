# Las tecnologías del proyecto y cómo encajan

Documento de referencia sobre el *stack* de la plataforma: qué hace cada pieza,
por qué está ahí, qué se rompería sin ella y qué alternativas existen.

Está pensado como material de apoyo para los capítulos de estado del arte y de
stack tecnológico de la memoria, y como recordatorio de trabajo.

---

## 1. El problema de fondo

Para entender por qué hacen falta tantas piezas conviene ver primero qué problema
resuelven juntas.

Una base de datos como PostgreSQL está diseñada para **transacciones**:
operaciones pequeñas, muy frecuentes, que leen o modifican unas pocas filas. Es
lo que se llama una carga **OLTP** (*online transaction processing*). Funciona
extraordinariamente bien para eso.

Pero cuando alguien quiere responder a «¿cuál ha sido el importe medio por
ciudad en los últimos seis meses?», la misma base de datos tiene que leer
millones de filas. Eso es una carga **OLAP** (*online analytical processing*), y
tiene dos problemas: es lenta sobre un motor orientado a filas, y compite por los
recursos de la base de datos que está atendiendo a los clientes reales.

La solución clásica fue el **ETL nocturno**: cada madrugada, un proceso copia los
datos a un almacén analítico. Funciona, pero tiene un coste: **los datos tienen
hasta 24 horas de antigüedad**. Para detectar un fraude, eso no sirve de nada.

De ahí las dos ideas que vertebran este proyecto:

1. **Capturar los cambios según ocurren** en vez de copiarlos por lotes → CDC.
2. **Procesarlos según llegan** en vez de acumularlos → *streaming*.

Todo lo demás son consecuencias de estas dos decisiones.

---

## 2. El recorrido de una transacción

Seguir un solo evento de principio a fin es la mejor forma de ver para qué está
cada componente.

```text
[1] Generador Python
      │ INSERT ... ON CONFLICT
      ▼
[2] PostgreSQL ──escribe en──► [3] WAL (write-ahead log)
                                    │ decodificación lógica
                                    ▼
                              [4] Debezium  (sobre Kafka Connect)
                                    │ evento JSON
                                    ▼
                              [5] Kafka · topic fraude.public.estado_cuenta
                                    │
                                    ▼
                              [6] Spark Structured Streaming
                                    │ ficheros Parquet + metadatos
                                    ▼
                              [7] Iceberg  ──registra en──► [8] Catálogo REST
                                    │ ficheros
                                    ▼
                              [9] MinIO (almacenamiento de objetos)
```

### [1] El generador escribe una transacción

Un `INSERT ... ON CONFLICT DO UPDATE` sobre la tabla `estado_cuenta`. Como esa
tabla guarda **una fila por cuenta** (patrón *account shadow*), casi todas las
operaciones son `UPDATE`.

Esto es deliberado: un `UPDATE` es el caso interesante para el CDC, porque el
valor anterior se pierde en la base de datos y solo el flujo de cambios conserva
la historia completa.

### [2] y [3] PostgreSQL lo escribe primero en el WAL

Aquí está la clave de todo el mecanismo. Antes de modificar la tabla, PostgreSQL
anota la operación en el **WAL** (*write-ahead log*), un registro secuencial de
todo lo que le ocurre a la base de datos. Existe para poder recuperarse de un
corte de luz: si el servidor cae, al arrancar relee el WAL y reconstruye lo que
quedó a medias.

Lo interesante es que **ese registro ya contiene todos los cambios, en orden**.
No hay que preguntarle a la base de datos qué ha cambiado: ya está escrito. Solo
hay que saber leerlo.

Por eso el `docker-compose.yml` arranca PostgreSQL con `wal_level=logical`. Ese
ajuste hace que el WAL guarde información suficiente para reconstruir los cambios
a nivel de fila, y no solo a nivel de bloque de disco.

### [4] Debezium lee el WAL y publica eventos

**Debezium** es un conector de *Change Data Capture*. Se suscribe al WAL de
PostgreSQL mediante **decodificación lógica**, traduce cada cambio a un evento
JSON y lo publica en Kafka.

Lo importante es lo que **no** hace: no consulta la tabla, no ejecuta `SELECT`, no
necesita una columna `fecha_modificacion` ni *triggers*. La base de datos no se
entera de que la están observando, más allá del coste de retener un poco de WAL.
Esa es la diferencia esencial frente a un ETL por sondeo.

Cada evento es un «sobre» con esta forma:

```json
{
  "payload": {
    "before": { "importe": 43.90, "...": "..." },
    "after":  { "importe": 1204.55, "...": "..." },
    "op": "u",
    "source": { "lsn": 24857392, "ts_ms": 1789140736256, "table": "estado_cuenta" }
  }
}
```

- `before` y `after`: la fila antes y después. Un `UPDATE` lleva las dos.
- `op`: `c` insert, `u` update, `d` delete, `r` lectura de la instantánea inicial.
- `source`: metadatos de procedencia, incluido el **LSN** (*log sequence number*),
  la posición exacta dentro del WAL. Es lo que permite saber el orden real de los
  cambios y reanudar donde se dejó.

**Piezas de Debezium que aparecen en el proyecto:**

- **Replication slot** (`fraude_slot`): la marca que recuerda hasta dónde ha leído Debezium. PostgreSQL **conserva el WAL que un slot aún no ha consumido**, lo que garantiza que no se pierde nada aunque el conector se caiga. El reverso es que un slot abandonado hace crecer el disco sin límite, problema que ya se sufrió en este proyecto.
- **Publication** (`dbz_publication`): declara qué tablas se publican.
- **Plugin `pgoutput`**: el decodificador lógico incluido en PostgreSQL desde la versión 10, así que no hay que instalar extensiones.
- **`decimal.handling.mode: double`**: sin esto, los `NUMERIC` viajan codificados en Base64 y llegan a Spark como texto ilegible.

### Kafka Connect: dónde vive Debezium

Debezium no es un programa suelto: es un *plugin* de **Kafka Connect**, un
servicio que aloja conectores y se ocupa de arrancarlos, reiniciarlos si fallan y
recordar por dónde iban. Es el contenedor `tfg-connect`, y se gestiona por una API
REST — de ahí que el conector se registre con un `curl`.

Connect guarda su estado en tópicos de Kafka (`connect_configs`,
`connect_offsets`, `connect_status`). Por eso, cuando Kafka se queda sin datos, el
conector desaparece y hay que volver a registrarlo.

### [5] Kafka transporta y amortigua

**Apache Kafka** es un *log distribuido*. La imagen mental correcta no es una cola
de mensajes clásica, sino **un cuaderno en el que solo se puede escribir al
final**, y que varios lectores recorren de forma independiente a su propio ritmo.

Conceptos que importan aquí:

- **Topic**: el cuaderno. En este proyecto, `fraude.public.estado_cuenta`, nombrado por Debezium como `<prefijo>.<esquema>.<tabla>`.
- **Partición**: cada topic se divide en particiones, y **el orden solo está garantizado dentro de una partición**. Hoy el topic tiene 1 partición, luego hay orden total; con varias, el orden sería solo por clave.
- **Clave**: Debezium usa la clave primaria (`id_cuenta`). Al repartir por clave, todos los eventos de una misma cuenta caen en la misma partición y **conservan su orden entre sí**, aunque haya varias particiones. Esto importa mucho aquí: calcular la media móvil de una cuenta con sus transacciones desordenadas daría resultados sin sentido.
- **Offset**: la posición de cada evento. Un consumidor solo tiene que recordar su offset para saber por dónde iba.
- **Retención**: Kafka guarda los eventos un tiempo configurable **aunque ya se hayan leído**. Eso permite que un consumidor nuevo lea la historia desde el principio, o que uno que falló reprocese.

**Por qué hay un Kafka en medio y no se conecta Debezium directamente a Spark.**
Dos razones:

1. **Desacoplamiento.** Si Spark se cae o se reinicia, Debezium sigue publicando y no pierde nada. Sin Kafka, una caída de Spark pararía la captura o perdería eventos. Además, mañana se pueden añadir consumidores nuevos sin tocar lo existente — que es exactamente lo que hace este proyecto con sus dos jobs.
2. **Amortiguación.** Si llegan 700 eventos por segundo y el consumidor procesa 400, Kafka **acumula la diferencia en disco** en vez de perderla. El consumidor la drena cuando el pico pasa. Es el mecanismo de absorción de picos que el TFG evalúa, y lo que hace que el *consumer lag* sea una métrica con sentido.

**ZooKeeper** (`tfg-zookeeper`) se ocupa de la coordinación del clúster: quién es
el líder de cada partición, qué brókers están vivos. Las versiones modernas de
Kafka pueden prescindir de él con el modo **KRaft**, pero las imágenes de Debezium
2.7 que usa el proyecto siguen el esquema clásico.

### [6] Spark Structured Streaming procesa

**Apache Spark** es un motor de procesamiento distribuido. **Structured Streaming**
es su API de flujo continuo, y su idea central es engañosamente simple: **tratar un
flujo infinito como una tabla que no para de crecer**. Se escribe la misma consulta
que se escribiría sobre datos estáticos, y Spark se encarga de ejecutarla una y
otra vez sobre lo nuevo.

Por debajo funciona por **micro-lotes**: cada cierto intervalo —el *trigger*—
recoge lo que haya llegado y lo procesa de golpe. No es streaming evento a evento,
y eso tiene una consecuencia directa: **la latencia mínima es el intervalo del
trigger**. Con un trigger de 10 s, un evento que llega justo después de un
micro-lote espera al siguiente. En las mediciones de este proyecto, ese intervalo
es la mayor parte de la latencia observada.

Piezas que aparecen en el código:

- **Checkpoint**: el directorio donde Spark anota qué offsets de Kafka ha procesado. Es lo que le permite reanudar exactamente donde lo dejó tras un reinicio. **Spark no confirma sus offsets contra Kafka**, los guarda aquí, y por eso `kafka-consumer-groups.sh` no ve a estos consumidores.
- **`maxOffsetsPerTrigger`**: cuántos eventos entran como máximo en cada micro-lote. Sin él, Spark consume todo lo disponible de una vez.
- **`foreachBatch`**: permite ejecutar código arbitrario sobre cada micro-lote, tratándolo como un DataFrame normal. Es lo que usa el detector de anomalías para cruzar el lote actual con el histórico.

### [7] Iceberg da forma de tabla a un montón de ficheros

Aquí está el concepto que más cuesta al principio.

Si se vuelcan ficheros Parquet en un almacén de objetos, lo que queda es **un
montón de ficheros**: no hay `UPDATE`, no hay transacciones, y si alguien lee
mientras otro escribe puede encontrarse datos a medias.

**Apache Iceberg** es un **formato de tabla abierto**: una capa de metadatos por
encima de esos ficheros que les da comportamiento de tabla de verdad. No es un
motor ni un servidor; son ficheros de metadatos que describen qué ficheros de
datos componen la tabla en cada momento.

Lo que aporta:

- **Transaccionalidad (ACID).** Cada escritura crea una **instantánea** nueva. Publicarla consiste en cambiar de forma atómica un puntero al fichero de metadatos vigente. Quien esté leyendo sigue viendo la versión anterior, coherente, hasta que termina. No hay lecturas a medias.
- **Evolución de esquema.** Añadir una columna es un cambio de metadatos, no una reescritura de los datos. Las filas antiguas devuelven `NULL`. Este proyecto lo usa: la función `asegurar_columnas` añadió la etiqueta de verdad a tablas que ya existían, sin tocar un solo fichero de datos.
- **Viaje en el tiempo.** Como cada instantánea se conserva, se puede consultar la tabla tal y como estaba en un momento dado.
- **Particionado oculto.** Iceberg sabe qué ficheros pueden contener las filas que cumplen un filtro y se salta el resto, sin que la consulta tenga que mencionar las columnas de particionado.

Las alternativas equivalentes son **Delta Lake** y **Apache Hudi**. Iceberg se
eligió por ser un estándar abierto con amplio soporte de motores y por no estar
ligado a un proveedor concreto.

### [8] El catálogo dice dónde está cada tabla

Los metadatos de Iceberg viven junto a los datos, pero algo tiene que saber
**cuál es el fichero de metadatos vigente** de `demo.bronze.eventos_cuenta`. De eso
se ocupa el **catálogo** (`tfg-iceberg-rest`).

Es una pieza pequeña pero crítica: mantiene, por cada tabla, un puntero a su
metadato actual, y lo cambia de forma atómica en cada *commit*. Si se pierde el
catálogo, **los datos siguen en MinIO pero dejan de ser alcanzables como tabla** —
problema que este proyecto sufrió antes de dar persistencia a su base de datos
interna.

El prefijo `demo.` de todas las tablas es sencillamente el nombre con el que este
catálogo está registrado en la sesión de Spark.

### [9] MinIO guarda los ficheros

**MinIO** es un almacén de objetos compatible con la API S3 de Amazon. Guarda
ficheros identificados por una clave, sin estructura de directorios real y sin
posibilidad de modificar un fichero a medias: se escribe entero o no se escribe.

Está aquí porque el modelo de un *data lakehouse* asume almacenamiento de objetos,
y porque permite desarrollar en local contra la misma API que se usaría en la nube
sin pagar nada. Cambiar MinIO por S3 real sería, en lo esencial, cambiar una URL.

---

## 3. La arquitectura por capas

Sobre ese almacenamiento, los datos se organizan en tres niveles, un patrón
conocido como **arquitectura medallón**:

| Capa | Qué contiene | Estado en el proyecto |
|---|---|---|
| **Bronze** | Los eventos tal y como llegaron, sin limpiar. Es el registro histórico fiel. | Construida |
| **Silver** | Datos validados, limpios y enriquecidos. Aquí viven las transacciones con su z-score y su marca de anomalía. | Construida por Spark |
| **Gold** | Agregados listos para consumir: métricas por cuenta, por ciudad, por periodo. | Pendiente |

La razón de guardar `bronze` sin tocar es práctica: **si se descubre un error en
la lógica de transformación, se puede rehacer todo desde los datos crudos**. Si
solo se guardara el resultado procesado, ese error sería irreversible.

---

## 4. Lo que aún no está construido

### dbt Core — transformaciones ELT

**dbt** permite escribir transformaciones como **consultas `SELECT` en ficheros
`.sql`**. Cada fichero es un «modelo», dbt averigua el orden de dependencias entre
ellos y los ejecuta contra el motor (aquí sería Spark SQL).

El nombre viene de **ELT** frente a **ETL**: en vez de transformar antes de
cargar, se cargan los datos crudos y se transforman dentro del propio almacén,
aprovechando su potencia de cálculo.

Aporta, además de la ejecución ordenada, **tests** declarativos (unicidad, no
nulos, valores permitidos), **documentación** y **linaje** generados a partir del
propio código.

### Apache Airflow — orquestación

Programa y supervisa flujos de trabajo, descritos como **DAG**: grafos de tareas
con sus dependencias. Se ocupa de ejecutarlas en orden, reintentar las que fallan
y dar visibilidad de qué corrió y cuándo.

Aquí se encargaría de lanzar dbt periódicamente. Conviene tener presente que
Airflow **no es ligero**, y que Spark ya consume más de la mitad de la memoria que
Docker tiene asignada en la máquina de desarrollo.

### DuckDB — consulta analítica

Un motor analítico **embebido**: no es un servidor, es una biblioteca que corre
dentro del proceso que la usa. Suele describirse como «el SQLite de la analítica».
Es columnar y muy rápido para agregaciones, y puede leer directamente ficheros
Parquet.

Está previsto como motor de consulta ligero sobre el lakehouse, evitando levantar
algo tan pesado como Trino.

### Metabase o Streamlit — visualización

**Metabase** es una herramienta de *business intelligence* lista para usar:
se conecta a una fuente y permite construir cuadros de mando pinchando. **Streamlit**
es una biblioteca de Python para construir aplicaciones de datos escribiendo
código. Metabase da más por defecto; Streamlit es más ligero y más programable, y
encaja mejor si el panel debe mostrar métricas del propio pipeline y no solo datos
de negocio. La elección está pendiente.

---

## 5. Patrones, no tecnologías

Dos ideas del proyecto que no son un producto concreto, sino formas de hacer:

**Dead-letter queue (DLQ).** Cuando llega un evento que no se puede procesar —un
campo que falta, un tipo incorrecto— hay tres salidas posibles: parar el pipeline,
descartarlo en silencio, o **apartarlo a un sitio donde quede registrado y se pueda
revisar**. La tercera es la DLQ. En este proyecto sería un tópico de Kafka
dedicado. Todavía no existe: hoy los eventos que no encajan se descartan sin dejar
rastro, que es justamente lo que hay que corregir.

**Account shadow.** Una fila por cuenta que refleja su estado más reciente, en vez
de un histórico de transacciones. Es una variante del *device shadow* habitual en
sistemas de IoT. Maximiza los `UPDATE`, que es lo que hace interesante el CDC, y
obliga a que el histórico completo viva en la capa `bronze`.

---

## 6. Resumen en una frase por pieza

| Tecnología | En una frase | Sin ella… |
|---|---|---|
| **PostgreSQL** | La base de datos donde ocurren las transacciones. | No hay origen de datos. |
| **WAL / decodificación lógica** | El registro donde PostgreSQL anota cada cambio antes de aplicarlo. | No habría de dónde capturar los cambios sin consultar la tabla. |
| **Debezium** | Lee ese registro y convierte cada cambio en un evento. | Habría que sondear la tabla periódicamente, con más carga y menos fidelidad. |
| **Kafka Connect** | El servicio que aloja y supervisa a Debezium. | Habría que gestionar el ciclo de vida del conector a mano. |
| **Apache Kafka** | Transporta los eventos y los amortigua cuando llegan más rápido de lo que se procesan. | Una caída del consumidor perdería eventos y no habría absorción de picos. |
| **ZooKeeper** | Coordina el clúster de Kafka. | Kafka no sabría quién lidera cada partición. |
| **Spark Structured Streaming** | Procesa los eventos por micro-lotes según llegan. | No habría transformación ni detección en tiempo real. |
| **Apache Iceberg** | Convierte un montón de ficheros en tablas con transacciones y evolución de esquema. | Habría ficheros sueltos, sin garantías ni forma de cambiar el esquema. |
| **Catálogo REST** | Sabe cuál es el estado vigente de cada tabla. | Los datos existirían pero no serían alcanzables por nombre. |
| **MinIO** | Almacena los ficheros con la API de S3. | No habría dónde poner el lakehouse. |
| **Docker Compose** | Levanta y conecta las nueve piezas con una orden. | Instalar y configurar todo a mano, sin reproducibilidad. |
| **dbt Core** | Transformaciones como SQL versionado, ordenado y testeado. | Las transformaciones serían scripts sueltos sin dependencias ni tests. |
| **Airflow** | Ejecuta y supervisa esas transformaciones periódicamente. | Habría que lanzarlas a mano. |
| **DuckDB** | Motor analítico embebido para consultar el lakehouse. | Haría falta un motor pesado como Trino. |
| **Metabase / Streamlit** | La capa visual sobre los datos. | Los resultados solo se verían por consola. |

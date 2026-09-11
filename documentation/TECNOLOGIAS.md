# Las tecnologías del proyecto

Qué hace cada pieza del proyecto y por qué está ahí.

Sirve de apoyo para los capítulos de estado del arte y de stack tecnológico de la
memoria.

---

## 1. Por qué hacen falta tantas piezas

PostgreSQL sirve para guardar transacciones. Operaciones pequeñas y muy
frecuentes. Eso lo hace muy bien.

Pero si preguntas "¿cuánto se ha gastado de media en Madrid en seis meses?", tiene
que leer millones de filas. Va lento. Y mientras lo hace, molesta a los clientes
que están usando la base de datos de verdad.

La solución de toda la vida era copiar los datos de madrugada a otro sitio. El
problema: tus datos tienen hasta 24 horas de antigüedad. **Para detectar un fraude
eso no vale.**

De ahí las dos ideas del proyecto:

1. Capturar los cambios **cuando pasan**, no de madrugada.
2. Procesarlos **cuando llegan**, no acumulados.

Todo lo demás sale de estas dos.

---

## 2. El camino de una transacción

```text
Generador Python
      │
      ▼
PostgreSQL  ──apunta el cambio en──►  su registro interno (WAL)
                                              │
                                              ▼
                                          Debezium
                                              │  evento JSON
                                              ▼
                                           Kafka
                                              │
                                              ▼
                                   Spark Structured Streaming
                                              │
                                              ▼
                                     Iceberg  ──►  Catálogo
                                              │
                                              ▼
                                            MinIO
```

### 1. El generador escribe

Un `UPDATE` sobre la fila de una cuenta. Como solo guardas una fila por cuenta, el
valor anterior se pierde. La historia completa solo existe en el flujo de cambios.

### 2. PostgreSQL apunta el cambio antes de hacerlo

Antes de tocar la tabla, PostgreSQL escribe lo que va a hacer en un cuaderno
interno: el **WAL**. Lo hace para poder recuperarse si se va la luz.

Aquí está el truco de todo el proyecto: **ese cuaderno ya tiene todos los cambios,
en orden**. No hace falta preguntarle nada a la base de datos. Solo hay que leerlo.

Por eso arrancas PostgreSQL con `wal_level=logical`: para que el cuaderno guarde
los cambios fila a fila y se puedan entender desde fuera.

### 3. Debezium lee ese cuaderno

Debezium se suscribe al WAL y convierte cada cambio en un mensaje JSON que manda a
Kafka.

Lo importante es lo que **no** hace: no lanza ni un `SELECT`, no necesita columnas
de fecha ni *triggers*. La base de datos casi ni se entera.

Cada mensaje trae la fila antes del cambio, la fila después, qué tipo de operación
fue (alta, modificación o borrado) y de dónde viene.

Debezium guarda una marca en PostgreSQL para recordar por dónde iba. Mientras esa
marca exista, PostgreSQL **no borra** el cuaderno pendiente. Por eso no se pierde
nada si Debezium se cae. Y por eso una marca abandonada te llena el disco, como
pasó con `iot_slot`.

### 4. Kafka guarda y reparte

Kafka recibe los mensajes y los guarda. Los consumidores los leen a su ritmo.

Está en medio por dos razones:

- **Si Spark se cae, no se pierde nada.** Debezium sigue escribiendo en Kafka y
  Spark recupera cuando vuelve.
- **Aguanta los picos.** Si llegan 700 mensajes por segundo y solo procesas 400,
  Kafka guarda la diferencia en disco. No se tira nada.

### 5. Spark procesa

Spark lee de Kafka, transforma y escribe. No trabaja mensaje a mensaje: junta lo
que ha llegado y lo procesa en tandas. A esas tandas se les llama micro-lotes.

Eso tiene una consecuencia directa: **si las tandas son cada 10 segundos, un
mensaje espera hasta 10 segundos**. Tus 10,89 segundos de latencia media eran casi
todo eso.

Spark apunta en un fichero por qué mensaje va. Así, si se reinicia, sigue donde lo
dejó.

### 6. Iceberg ordena los ficheros

Spark escribe ficheros. Iceberg lleva la cuenta de qué ficheros forman cada tabla.

Sin Iceberg tendrías un montón de ficheros sueltos: sin `UPDATE`, sin garantías, y
si alguien lee mientras otro escribe puede ver datos a medias.

Con Iceberg, cada escritura crea una versión nueva de la tabla. Publicarla es
cambiar un único puntero. Quien esté leyendo sigue viendo la versión anterior
entera hasta que termina.

Eso te da tres cosas:

- **Añadir columnas sin reescribir nada.** Las filas viejas devuelven vacío. Es lo
  que hiciste al añadir la etiqueta de verdad.
- **Consultar la tabla como estaba antes**, porque las versiones se guardan.
- **Leer menos.** Iceberg sabe qué hay en cada fichero y se salta los que no
  necesita.

### 7. El catálogo y MinIO

El **catálogo** guarda una sola cosa: dónde está la versión actual de cada tabla.
Es minúsculo, pero si lo pierdes, los datos siguen en MinIO y ya no se pueden
consultar. Por eso le pusimos un volumen.

**MinIO** es donde viven los ficheros. Funciona como el almacenamiento de Amazon
S3, pero en tu máquina y gratis.

---

## 3. Las tres capas

| Capa | Qué guarda | Estado |
|---|---|---|
| **Bronze** | Los eventos tal cual llegaron | Hecha |
| **Silver** | Datos limpios, con el z-score y la marca de anomalía | Hecha |
| **Gold** | Resúmenes por cuenta, ciudad o periodo | Pendiente |

Bronze se guarda sin tocar por un motivo práctico: **si te equivocas en la lógica,
rehaces todo desde ahí**. Si solo guardaras el resultado, el error sería para
siempre.

---

## 4. Glosario de tecnologías

### Ya integradas

**PostgreSQL** · Base de datos donde ocurren las transacciones. Es el origen de
todo el pipeline.

**Debezium** · Lee el registro interno de PostgreSQL y convierte cada cambio en un
mensaje. Es lo que hace posible capturar los cambios sin consultar la tabla.

**Kafka Connect** · El servicio donde vive Debezium. Lo arranca, lo reinicia si
falla y recuerda por dónde iba. Se le habla por una API web, de ahí los `curl`.

**Apache Kafka** · Guarda los mensajes en orden y deja que varios programas los
lean a su ritmo. Hace de amortiguador cuando llegan más rápido de lo que se
procesan.

**ZooKeeper** · Le lleva las cuentas internas al clúster de Kafka. Las versiones
modernas de Kafka ya no lo necesitan, pero las imágenes que usas sí.

**Apache Spark** · Motor de procesamiento. Lee de Kafka, transforma los datos,
detecta anomalías y escribe el resultado.

**Spark Structured Streaming** · La parte de Spark que trabaja sobre flujos
continuos. Procesa por tandas en vez de mensaje a mensaje.

**Apache Iceberg** · Lleva la cuenta de qué ficheros forman cada tabla. Es lo que
convierte un montón de ficheros sueltos en tablas de verdad, con versiones y con
posibilidad de cambiar el esquema.

**Parquet** · El formato de los ficheros de datos. Guarda por columnas en vez de
por filas, así que ocupa mucho menos y puede leer solo las columnas que pides.

**Catálogo REST de Iceberg** · Apunta dónde está la versión actual de cada tabla.
Sin él, los datos existen pero no se pueden consultar por su nombre.

**MinIO** · Almacena los ficheros. Habla el mismo idioma que Amazon S3, así que
migrar a la nube sería cambiar una dirección.

**Docker y Docker Compose** · Levantan las nueve piezas con una sola orden y hacen
que el proyecto se pueda reproducir en otro ordenador.

**Python** · El lenguaje del generador y de los scripts de Spark.

**Faker** · Librería que inventa datos falsos con pinta real.

**psycopg2** · La librería con la que Python habla con PostgreSQL.

### Pendientes de integrar

**dbt Core** · Permite escribir las transformaciones como consultas SQL en
ficheros. Averigua solo en qué orden hay que ejecutarlas y puede comprobar que los
datos cumplen lo que esperas.

**Apache Airflow** · Lanza esas transformaciones cada cierto tiempo y avisa si
alguna falla. Consume bastante memoria, conviene comprobar que cabe.

**DuckDB** · Motor para hacer consultas rápidas sobre el lakehouse. No es un
servidor: es una librería que corre dentro de tu programa.

**Metabase** · Herramienta de paneles. Se conecta a los datos y construyes
gráficos pinchando, sin programar.

**Streamlit** · Librería de Python para hacer paneles escribiendo código. Más
ligera que Metabase y más flexible.

### Descartadas o alternativas

**Redpanda** · Alternativa a Kafka, más ligera. Se guardaba por si Kafka no cabía
en la máquina. No ha hecho falta.

**Trino** · Motor de consultas distribuido. Descartado por pesado: DuckDB hace lo
que necesitas.

**Delta Lake y Apache Hudi** · Las dos alternativas a Iceberg. Se eligió Iceberg
por ser estándar abierto y no depender de ningún fabricante.

---

## 5. Dos ideas que no son tecnología

**Dead-letter queue** · Cuando llega un mensaje roto tienes tres opciones: parar
todo, tirarlo sin más, o apartarlo a un sitio donde quede guardado para revisarlo.
La tercera es la dead-letter queue. Ahora mismo tu pipeline hace la segunda.

**Account shadow** · Guardar una fila por cuenta con su última transacción, en vez
de una fila por transacción. Así casi todo son modificaciones, que es lo que hace
interesante capturar los cambios.

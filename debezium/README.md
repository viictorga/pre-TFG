# Captura de cambios (CDC) con Debezium

Esta carpeta contiene la configuración del conector que captura los cambios de
la tabla `estado_cuenta` en PostgreSQL y los publica como eventos en Kafka.

Para levantar el entorno completo y el resto del pipeline, consulta el
[`README.md`](../README.md) de la raíz del repositorio. Aquí solo se documenta
la pieza de CDC.

## Qué hace el conector

Cada vez que el generador actualiza la fila de una cuenta en `estado_cuenta`,
Debezium lee esa modificación del *write-ahead log* de PostgreSQL —sin
consultar la tabla ni añadir carga a la base de datos— y publica un evento JSON
en el topic `fraude.public.estado_cuenta`.

Esa es la diferencia frente a una extracción por lotes: el evento aparece en
Kafka en cuestión de milisegundos desde el `UPDATE`, y la base de datos origen
no se entera.

## Configuración: `register-connector.json`

| Clave | Valor | Por qué |
|---|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Conector de PostgreSQL |
| `database.*` | `postgres:5432`, base `iot`, usuario `tfg` | El host es el nombre del servicio en la red de Docker, no `localhost` |
| `topic.prefix` | `fraude` | Primer segmento del nombre del topic: `fraude.public.estado_cuenta` |
| `table.include.list` | `public.estado_cuenta` | Solo se captura esa tabla |
| `plugin.name` | `pgoutput` | Plugin de decodificación lógica incluido en PostgreSQL desde la versión 10; no hay que instalar nada |
| `slot.name` | `fraude_slot` | Nombre del *replication slot* que Debezium crea en Postgres |
| `publication.autocreate.mode` | `filtered` | Crea la publicación solo para las tablas de `table.include.list` |
| `decimal.handling.mode` | `double` | Sin esto, los `NUMERIC` viajan como bytes codificados en Base64 y Spark no los puede leer como número |

El requisito previo del lado de PostgreSQL es `wal_level=logical`, que ya viene
puesto en el `command` del servicio `postgres` en `docker-compose.yml`.

## Registrar el conector

Con el entorno levantado y `tfg-connect` arrancado del todo (tarda 20-30
segundos más que el resto), desde la raíz del repositorio:

```bash
curl -s http://localhost:8083/connectors
```

Devuelve `[]` mientras no haya ningún conector registrado. Para registrarlo:

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium/register-connector.json
```

Respuesta esperada: `HTTP/1.1 201 Created`. Comprobar su estado:

```bash
curl -s localhost:8083/connectors/fraude-connector/status
```

Tanto el conector como su tarea deben aparecer en `"state":"RUNNING"`.

## Ver los eventos CDC en directo

Con el generador corriendo en otra terminal:

```bash
docker exec -it tfg-kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic fraude.public.estado_cuenta \
  --from-beginning
```

> Dentro del contenedor hay que usar `kafka:9092`, no `localhost:9092`: el
> broker anuncia su nombre de servicio y con `localhost` la herramienta se queda
> reintentando hasta agotar el tiempo de espera.

> En Git Bash sobre Windows, exporta antes `MSYS_NO_PATHCONV=1` o usa
> PowerShell: Git Bash convierte `/kafka/bin/...` en una ruta de Windows y el
> `docker exec` falla con `no such file or directory`.

Cada evento es un sobre JSON con esta forma (recortada):

```json
{
  "payload": {
    "before": { "...": "estado anterior de la fila" },
    "after": {
      "id_cuenta": "cuenta-012",
      "tipo_transaccion": "compra_online",
      "importe": 43.9,
      "ubicacion": "Madrid",
      "resultado": "aprobada"
    },
    "op": "u",
    "source": { "...": "metadatos: LSN, timestamp, tabla de origen" }
  }
}
```

El campo `op` indica la operación: `c` (create/insert), `u` (update), `d`
(delete) y `r` (read, la instantánea inicial). Los scripts de Spark leen
`payload.after` y descartan el resto del sobre.

## Gestión del conector

```bash
# Listar conectores registrados
curl -s localhost:8083/connectors

# Reiniciar el conector (no borra el replication slot ni los offsets)
curl -X POST localhost:8083/connectors/fraude-connector/restart

# Eliminar el conector
curl -X DELETE localhost:8083/connectors/fraude-connector
```

**Precaución:** eliminar el conector no elimina su *replication slot* en
PostgreSQL. Un slot huérfano hace que PostgreSQL conserve indefinidamente los
segmentos del WAL que ese slot aún no ha consumido, y el disco se va llenando
sin motivo aparente. Para comprobarlos y, si hace falta, liberarlos:

```bash
docker exec -it tfg-postgres psql -U tfg -d iot \
  -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"
```

## Problemas habituales

- **El `POST` devuelve 500 o un error de *replication slot*:** Postgres o Kafka
  todavía no habían terminado de arrancar. Espera unos segundos y reinténtalo;
  es una condición de carrera típica del primer arranque.
- **`tfg-connect` se reinicia en bucle:** revisa `docker logs tfg-connect`. Casi
  siempre es que Kafka no estaba listo cuando Connect intentó conectarse.
- **El topic no existe al intentar consumirlo:** se crea con el primer evento.
  Comprueba que el generador está corriendo y que el conector está en `RUNNING`.
- **Los importes llegan como texto raro tipo `"BCg="`:** falta
  `decimal.handling.mode: double` en la configuración del conector.

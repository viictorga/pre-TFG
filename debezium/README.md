# TFG — Paso 1: Postgres + generador de dispositivos IoT

Primera pieza del pipeline: una base de datos Postgres con una tabla
`estado_dispositivo` (patrón *device shadow*) y un generador en Python que
simula ~30 sensores IoT actualizando su lectura cada pocos segundos.

Esto es intencionadamente lo mínimo posible — todavía no hay Kafka, ni
Debezium, ni Spark. Es la base sobre la que se construye todo lo demás.

## Requisitos

- Docker y Docker Compose instalados
- Python 3.9+

## 1. Levantar Postgres

```bash
docker compose up -d
docker compose ps        # comprueba que aparece "healthy"
```

Esto crea la base de datos `iot`, el usuario `tfg` y la tabla
`estado_dispositivo` automáticamente (usando `sql/init.sql`). También deja
Postgres configurado con `wal_level=logical`, que es lo que necesitará
Debezium más adelante para el CDC — así no hay que tocar esto otra vez.

## 2. Instalar y lanzar el generador

```bash
cd generator
python -m venv venv
source venv/bin/activate        # en Windows: venv\Scripts\activate
pip install -r requirements.txt
python generate_devices.py
```

Deberías ver algo como:

```
sensor-005   temperatura  24.67C
sensor-007   humedad      62.48%
sensor-001   humedad      13.21%  <-- ANOMALIA
```

Para el generador con `Ctrl+C` cuando quieras.

## 3. Comprobar los datos

En otra terminal:

```bash
docker exec -it tfg-postgres psql -U tfg -d iot -c "SELECT * FROM estado_dispositivo ORDER BY fecha_actualizacion DESC LIMIT 10;"
```

Deberías ver **una fila por dispositivo** (no una por lectura) con el valor
y el timestamp más recientes — eso es lo que hace que capturar solo los
`UPDATE` con CDC tenga sentido en el siguiente paso.

## Variables de entorno del generador

| Variable | Por defecto | Qué hace |
|---|---|---|
| `NUM_DISPOSITIVOS` | 30 | Cuántos sensores simular |
| `INTERVALO_SEGUNDOS` | 2 | Segundos entre lecturas |
| `PROB_ANOMALIA` | 0.04 | Probabilidad de generar una lectura fuera de rango |

Ejemplo para probar más rápido y con más anomalías:

```bash
NUM_DISPOSITIVOS=10 INTERVALO_SEGUNDOS=0.5 PROB_ANOMALIA=0.2 python generate_devices.py
```

## Parar y limpiar

```bash
docker compose down       # para Postgres, conserva los datos
docker compose down -v    # para Postgres y borra los datos
```

## Siguiente paso

Con esto funcionando, el siguiente paso del cronograma es añadir Debezium +
Kafka y comprobar que cada `UPDATE` sobre `estado_dispositivo` aparece como
un evento CDC — todavía sin Spark ni Iceberg.

---

# Paso 2: Debezium + Kafka (captura de cambios en streaming)

> **Nota honesta:** esta parte sigue el patrón oficial de los tutoriales de
> Debezium casi al pie de la letra (es el más fiable que existe para esto),
> pero no he podido ejecutarla yo mismo de extremo a extremo porque mi
> entorno de pruebas no tiene acceso a Docker Hub / quay.io. Si algo falla,
> dímelo con el mensaje de error exacto y lo resolvemos juntos.

## 1. Levantar Kafka y Debezium

Con el nuevo `docker-compose.yml` (ya incluye Zookeeper, Kafka y Connect):

```bash
docker compose up -d
docker compose ps
```

Deberías ver 4 contenedores: `tfg-postgres`, `tfg-zookeeper`, `tfg-kafka` y
`tfg-connect`. Dale 20-30 segundos a `tfg-connect` para arrancar del todo —
es el que más tarda.

## 2. Registrar el conector

Comprueba que Connect está despierto:

```bash
curl -s http://localhost:8083/connectors
```

Debería devolver `[]` (ningún conector registrado todavía). Ahora regístralo:

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  localhost:8083/connectors/ -d @debezium/register-connector.json
```

Una respuesta `201 Created` significa que se ha registrado bien. Comprueba
su estado:

```bash
curl -s localhost:8083/connectors/iot-connector/status
```

Busca `"state":"RUNNING"` tanto en el conector como en la tarea.

## 3. Ver los eventos CDC en directo

Con el generador (`generate_devices.py`) corriendo en otra terminal, consume
el topic donde Debezium publica los cambios:

```bash
docker exec -it tfg-kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot.public.estado_dispositivo \
  --from-beginning
```

Cada vez que el generador actualice un dispositivo, debería aparecer aquí
un evento JSON nuevo casi al instante — esa latencia mínima entre el UPDATE
en Postgres y el evento en Kafka es exactamente lo que hace valioso el CDC
frente a una extracción por lotes.

## Solución de problemas habituales

- **El POST del conector devuelve `500` o un error de "replication slot"**:
  espera unos segundos más a que Postgres y Kafka terminen de arrancar y
  reinténtalo — es una condición de carrera típica en el primer arranque.
- **`tfg-connect` se reinicia en bucle**: revisa sus logs con
  `docker logs tfg-connect` — casi siempre es que Kafka todavía no estaba
  listo cuando Connect intentó conectarse.
- **El topic no existe cuando intentas consumirlo**: el topic se crea solo
  cuando llega el primer evento. Asegúrate de que el generador está
  corriendo y de que el conector está en estado `RUNNING`.

## Siguiente paso

Con los eventos CDC llegando a Kafka, el siguiente bloque del cronograma es
Spark Structured Streaming: consumirlos, detectar anomalías por ventana
deslizante, y escribirlos en la capa bronze del lakehouse (Iceberg).

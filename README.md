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

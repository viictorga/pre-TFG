-- Tabla "device shadow": una fila por dispositivo con su lectura más reciente.
-- Cada UPDATE sobre esta tabla es justo lo que Debezium capturará más adelante.

CREATE TABLE IF NOT EXISTS estado_dispositivo (
    id_dispositivo        VARCHAR(20) PRIMARY KEY,
    tipo_sensor            VARCHAR(20) NOT NULL,
    ubicacion               VARCHAR(50) NOT NULL,
    valor                    NUMERIC(10, 2) NOT NULL,
    unidad                  VARCHAR(10) NOT NULL,
    estado                  VARCHAR(10) NOT NULL DEFAULT 'activo',
    fecha_actualizacion     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_estado_dispositivo_tipo ON estado_dispositivo (tipo_sensor);
CREATE INDEX IF NOT EXISTS idx_estado_dispositivo_ubicacion ON estado_dispositivo (ubicacion);

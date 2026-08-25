-- Tabla "device shadow" para cuentas bancarias: una fila por cuenta con su
-- transaccion mas reciente. Cada UPDATE es justo lo que Debezium capturara
-- como evento CDC.

CREATE TABLE IF NOT EXISTS estado_cuenta (
    id_cuenta               VARCHAR(20) PRIMARY KEY,
    tipo_transaccion        VARCHAR(20) NOT NULL,   -- compra_online, compra_presencial, retirada_cajero, transferencia
    categoria_comercio      VARCHAR(20),             -- NULL en retirada_cajero y transferencia (no aplica)
    canal                   VARCHAR(20) NOT NULL,    -- online, presencial, cajero, app_movil
    ubicacion               VARCHAR(50) NOT NULL,
    importe                 NUMERIC(10, 2) NOT NULL,
    moneda                  VARCHAR(5) NOT NULL DEFAULT 'EUR',
    resultado               VARCHAR(10) NOT NULL,    -- aprobada, rechazada
    estado                  VARCHAR(10) NOT NULL DEFAULT 'activa',  -- activa, bloqueada
    fecha_actualizacion     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_estado_cuenta_tipo ON estado_cuenta (tipo_transaccion);
CREATE INDEX IF NOT EXISTS idx_estado_cuenta_ubicacion ON estado_cuenta (ubicacion);
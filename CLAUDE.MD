# CLAUDE.md — Memoria del proyecto (TFG)

Este archivo es la memoria persistente del proyecto para Claude Code.

Se lee automáticamente al inicio de cada sesión. Su objetivo es triple:

1. Que Claude Code entienda el proyecto sin que haya que reexplicarlo cada vez.
2. Fijar cómo debe trabajar (git, commits, cuándo pedir permiso).
3. Servir de materia prima para redactar la memoria académica del TFG.

> **IMPORTANTE:** El proyecto está en fase de diseño y todas las decisiones son provisionales hasta que se validen mediante implementación y experimentación. Claude Code no debe tratar ninguna decisión como inmutable salvo que esté marcada explícitamente como definitiva.

---

# 1. Objetivo del TFG

## 1.1 Enfoque general

El TFG se centra principalmente en **Ingeniería de Datos**, no en Ciencia de Datos.

El protagonista del proyecto debe ser una **plataforma de ingeniería de datos** capaz de ingerir, procesar, almacenar, transformar, consultar y monitorizar datos en tiempo real.

El caso de uso sirve como escenario para demostrar las capacidades de la plataforma, pero **no debe convertirse en el centro del proyecto**.

El objetivo es desarrollar una plataforma que se aproxime a una solución real de ingeniería de datos, incluyendo procesamiento de eventos, almacenamiento, transformación, monitorización, detección de anomalías y mecanismos de resiliencia.

El proyecto debe ir más allá de un simple pipeline de análisis de datos. La plataforma debe ser capaz de proporcionar funcionalidades operativas sobre los datos, como:

- Procesamiento de eventos en tiempo real.
- Detección de anomalías.
- Generación de alertas.
- Gestión de eventos corruptos.
- Absorción de picos de carga.
- Persistencia de datos históricos.
- Transformaciones y agregaciones.
- Consulta de información procesada.
- Visualización y monitorización.
- Evaluación cuantitativa del comportamiento del sistema.

## 1.2 Pregunta de investigación actual

La pregunta de investigación provisional seleccionada es:

> **¿Cómo afectan la carga de trabajo y distintos parámetros de configuración al rendimiento de una plataforma de ingeniería de datos desplegada íntegramente con Docker?**

Esta pregunta es **provisional y puede evolucionar** durante el diseño y la implementación.

La investigación debe centrarse en aspectos medibles de la plataforma, especialmente:

- Rendimiento.
- Latencia.
- Throughput.
- Escalabilidad.
- Consumo de recursos.
- Comportamiento ante incrementos de carga.
- Efecto de distintos parámetros de configuración.
- Comportamiento ante eventos corruptos.
- Integridad de los datos.

Se priorizarán experimentos reproducibles y cuantificables frente a conclusiones subjetivas.

## 1.3 Objetivo general provisional

Diseñar, implementar y evaluar un pipeline de datos de extremo a extremo que capture y procese cambios en tiempo real, detecte anomalías en el flujo de eventos y sea capaz de adaptarse a imprevistos operativos —principalmente picos de carga y eventos corruptos— sin perder disponibilidad, integrándolo en una arquitectura moderna de procesamiento y almacenamiento de datos, utilizando exclusivamente herramientas gratuitas y de código abierto.

## 1.4 Objetivos específicos provisionales

1. Diseñar un generador de datos sintéticos que permita producir eventos de transacciones bancarias y controlar artificialmente su volumen, frecuencia, anomalías y corrupción.
2. Implementar la captura de cambios (CDC) sobre la fuente de datos mediante Debezium y un sistema de mensajería basado en Kafka o una alternativa compatible.
3. Desarrollar un proceso de streaming que consuma los eventos capturados y los persista en una capa `bronze` mediante un formato de tabla abierto.
4. Implementar un mecanismo de detección de anomalías en tiempo real basado inicialmente en métodos estadísticos simples, como estadísticas por ventana deslizante.
5. Diseñar el pipeline para absorber picos de carga sin pérdida de eventos.
6. Evaluar el comportamiento de la plataforma ante distintos niveles de carga.
7. Implementar una cola de eventos fallidos (`dead-letter queue`) para aislar eventos corruptos o malformados sin detener el procesamiento del resto del flujo.
8. Definir e implementar transformaciones ELT hacia las capas `silver` y `gold`.
9. Incorporar mecanismos de orquestación y supervisión de las transformaciones.
10. Habilitar una capa de consulta y visualización que permita monitorizar el estado de los datos, las anomalías y los eventos fallidos.
11. Evaluar experimentalmente el sistema mediante pruebas controladas de carga y corrupción de eventos.
12. Analizar cómo distintos parámetros de configuración afectan al rendimiento de la plataforma.

---

# 2. Caso de uso

## 2.1 Caso de uso definido

El caso de uso del TFG es:

> **Detección de fraude en transacciones bancarias simuladas.**

Este caso de uso es concreto y forma parte de la definición del proyecto. Claude Code **no debe sustituirlo por un caso de uso industrial, energético, logístico, IoT o cualquier otro dominio** salvo que el usuario indique explícitamente que se quiere modificar.

El hecho de que la plataforma sea el protagonista del TFG no implica que el caso de uso sea genérico. La plataforma se diseñará y evaluará utilizando este escenario concreto.

## 2.2 Escenario de datos

El escenario simula un conjunto de cuentas bancarias que generan transacciones de distintos tipos de forma periódica.

Las transacciones podrán representar operaciones como:

- Compra online.
- Compra presencial.
- Retirada en cajero.
- Transferencia.

El sistema utilizará datos sintéticos para evitar depender de datos reales de clientes o de una entidad bancaria.

El generador de datos permitirá controlar:

- Número de cuentas.
- Frecuencia de generación.
- Volumen de eventos.
- Porcentaje de transacciones anómalas.
- Picos de carga.
- Porcentaje de eventos corruptos.

## 2.3 Detección de anomalías y fraude

La detección de anomalías se realizará durante el procesamiento del flujo de datos y no en el origen.

El generador debe poder inyectar de forma controlada transacciones con importes anómalos y otros patrones que permitan evaluar objetivamente el funcionamiento del sistema.

En una fase inicial se plantea utilizar métodos estadísticos sencillos, como estadísticas por ventana deslizante y z-score.

El objetivo no es desarrollar un sistema bancario real de detección de fraude ni un modelo complejo de Machine Learning, sino utilizar el caso de uso para evaluar las capacidades de la plataforma de ingeniería de datos.

## 2.4 Escenarios de estrés

El caso de uso también debe permitir evaluar el comportamiento de la plataforma ante situaciones anómalas del propio flujo de datos.

Se contemplan dos escenarios principales:

### Pico de carga

Se generan ráfagas de transacciones muy superiores a la frecuencia habitual para estudiar:

- Throughput.
- Latencia.
- Consumer lag.
- Capacidad de absorción del sistema.
- Comportamiento de Kafka.
- Comportamiento de Spark Structured Streaming.
- Posible pérdida de eventos.

### Eventos corruptos

Se generan deliberadamente eventos malformados, por ejemplo:

- Tipos incorrectos.
- Campos ausentes.
- Valores no numéricos.
- Estructuras inválidas.

Estos eventos deben ser aislados mediante una **dead-letter queue (DLQ)** sin detener el procesamiento del resto del flujo.

## 2.5 Principio fundamental del caso de uso

El caso de uso es **fijo salvo decisión explícita del usuario**.

La plataforma debe mantenerse suficientemente modular para que sus componentes de ingeniería puedan reutilizarse en otros contextos, pero esto no debe interpretarse como que el TFG tiene un caso de uso genérico.

Todas las decisiones de diseño, implementación y evaluación deben estar justificadas en relación con la **detección de fraude en transacciones bancarias simuladas**.

# 3. Arquitectura general

La arquitectura prevista sigue un modelo por capas:

```text
                    Generador de datos
                           │
                           ▼
                       PostgreSQL
                           │
                           │ CDC
                           ▼
                       Debezium
                           │
                           ▼
                    Apache Kafka
                           │
                           ▼
             Spark Structured Streaming
                           │
              ┌────────────┴─────────────┐
              │                          │
              ▼                          ▼
       Validación de eventos       Detección de
              │                    anomalías
              │
       ┌──────┴──────┐
       │             │
       ▼             ▼
    Eventos       Eventos
     válidos      corruptos
       │             │
       ▼             ▼
     Bronze          DLQ
       │
       ▼
     Silver
       │
       ▼
      Gold
       │
       ├──────────────► Consulta
       │
       └──────────────► Dashboard
```

La arquitectura definitiva podrá modificarse durante la implementación si las pruebas demuestran que una alternativa es técnicamente más adecuada.

## 3.1 Componentes previstos

### Fuente de datos

**PostgreSQL**

Base de datos que representa el sistema origen.

En este escenario almacenará el estado de las cuentas bancarias (patrón "account shadow": una fila por cuenta con su transacción más reciente) y/o los últimos eventos necesarios para demostrar la captura mediante CDC.

### Generación de datos

**Python**

Programa encargado de generar datos sintéticos.

Debe permitir controlar la carga y generar deliberadamente:

- Datos normales.
- Datos anómalos.
- Picos de carga.
- Eventos corruptos.

### Captura de cambios

**Debezium**

Componente encargado de capturar los cambios realizados en PostgreSQL y publicarlos como eventos.

### Mensajería

**Apache Kafka**

Actúa como sistema de mensajería y buffer entre la captura de cambios y el procesamiento.

Debe permitir estudiar:

- Particionado.
- Buffering.
- Throughput.
- Consumer lag.
- Comportamiento ante picos de carga.

**Redpanda** podrá utilizarse como alternativa si las limitaciones de recursos de la máquina de desarrollo hacen que Kafka sea demasiado pesado.

### Procesamiento

**Apache Spark Structured Streaming**

Consume los eventos procedentes de Kafka.

Responsabilidades previstas:

- Consumo de eventos.
- Validación.
- Transformación inicial.
- Detección de anomalías.
- Persistencia en la capa `bronze`.

### Dead-Letter Queue

Se utilizará un tópico Kafka dedicado para eventos que no superen las validaciones básicas.

Los eventos corruptos no deben detener el pipeline principal.

La DLQ debe permitir:

- Aislar eventos inválidos.
- Contabilizarlos.
- Inspeccionarlos.
- Mantener información suficiente para su posterior reprocesado.

### Data Lakehouse

Se utilizará una arquitectura por capas:

```text
Bronze
  ↓
Silver
  ↓
Gold
```

La capa `bronze` almacenará los datos crudos.

Las capas `silver` y `gold` contendrán datos progresivamente limpiados, validados, transformados y agregados.

### Formato de tabla

**Apache Iceberg**

Se plantea como formato abierto para las tablas del lakehouse.

### Almacenamiento de objetos

**MinIO**

Se utilizará como almacenamiento de objetos local compatible con el modelo S3.

### Transformaciones ELT

**dbt Core + Spark SQL**

Se utilizarán para implementar transformaciones hacia las capas `silver` y `gold`.

### Orquestación

**Apache Airflow**

Se plantea para programar y supervisar la ejecución periódica de transformaciones.

### Consulta

**DuckDB**

Se plantea como motor de consulta analítica ligero.

**Trino** no es prioritario y solo debería introducirse si resulta necesario y los recursos disponibles lo permiten.

### Visualización

Se plantea utilizar:

- Metabase Open Source, o
- Streamlit.

La elección definitiva se realizará durante la fase de implementación.

---

# 4. Plan por fases y estado actual

Las fases son provisionales y pueden modificarse.

Una fase solo debe marcarse como completada cuando haya sido **probada y funcione de extremo a extremo**, no simplemente porque el código exista.

**Estado a 2026-09-11.** Completas y verificadas: la **fase 3** (entorno base) y la **fase 6** (detección de anomalías, a falta de calibrar el umbral). Parciales: la **fase 1** (faltan el estado del arte, la validación con el tutor y la arquitectura definitiva), la **fase 4** (falta el modo de eventos corruptos) y la **fase 5** (falta separar los eventos corruptos). Sin empezar: las fases 2, 8, 9, 10 y 11. La **fase 7** tiene ya la instrumentación de medida construida, pero ningún escenario de estrés ejecutado.

## Fase 1 — Definición y estado del arte

- [x] Definir enfoque principal en Ingeniería de Datos.
- [x] Decidir que la plataforma será el protagonista del TFG.
- [x] Definir una pregunta de investigación provisional.
- [x] Establecer como requisito que todo el proyecto sea gratuito.
- [x] Establecer Docker como entorno de despliegue.
- [x] Confirmar el caso de uso (detección de fraude en transacciones bancarias simuladas) como definitivo.
- [ ] Completar el estado del arte.
- [ ] Validar definitivamente el alcance con el tutor.
- [ ] Diseñar la arquitectura técnica definitiva.

## Fase 2 — Diseño técnico

- [ ] Definir arquitectura definitiva.
- [ ] Definir flujo completo de datos.
- [ ] Definir modelo de datos.
- [ ] Definir esquema de eventos.
- [ ] Definir estrategia CDC.
- [ ] Definir estrategia de particionado.
- [ ] Definir estrategia de almacenamiento.
- [ ] Definir validaciones.
- [ ] Definir funcionamiento de la DLQ.
- [ ] Definir métricas de evaluación.
- [ ] Definir experimentos.

**Entregable:** documento de diseño técnico.

## Fase 3 — Entorno base

- [x] Configurar Docker Compose.
- [x] Configurar PostgreSQL.
- [x] Configurar Kafka o Redpanda.
- [x] Configurar MinIO.
- [x] Comprobar comunicación entre servicios.
- [x] Documentar cómo levantar y detener el entorno.

**Entregable:** entorno de desarrollo funcional.

## Fase 4 — Generador de datos y CDC

- [x] Implementar generador de datos de transacciones bancarias.
- [x] Implementar generación de eventos normales.
- [x] Implementar inyección controlada de anomalías.
- [x] Implementar modo de pico de carga.
- [ ] Implementar modo de eventos corruptos.
- [x] Configurar Debezium.
- [x] Comprobar captura de INSERT/UPDATE.
- [x] Comprobar publicación correcta en Kafka.

**Entregable:** datos sintéticos capturados como eventos CDC.

## Fase 5 — Streaming y capa Bronze

- [x] Implementar Spark Structured Streaming.
- [x] Consumir eventos desde Kafka.
- [ ] Validar estructura de eventos.
- [ ] Separar eventos válidos y corruptos.
- [x] Implementar escritura en Iceberg.
- [x] Construir capa `bronze`.

**Entregable:** pipeline streaming funcional hasta `bronze`.

## Fase 6 — Detección de anomalías

- [x] Definir método estadístico.
- [x] Implementar estadísticas por ventana.
- [x] Calcular media.
- [x] Calcular desviación.
- [x] Definir umbral inicial mediante z-score.
- [x] Marcar eventos normales/anómalos.
- [x] Generar información suficiente para evaluar precisión y exhaustividad.

**Entregable:** detección de anomalías en tiempo real.

## Fase 7 — Resiliencia y estrés

- [ ] Implementar DLQ.
- [ ] Implementar aislamiento de eventos corruptos.
- [ ] Comprobar que un evento corrupto no detiene el pipeline.
- [ ] Implementar escenarios de pico de carga.
- [ ] Medir consumer lag.
- [ ] Medir throughput.
- [ ] Medir latencia.
- [ ] Comprobar pérdida de eventos.
- [ ] Evaluar backpressure y capacidad de absorción.

**Entregable:** pipeline con mecanismos de resiliencia funcionando.

## Fase 8 — Transformaciones ELT

- [ ] Configurar dbt Core.
- [ ] Definir modelos `silver`.
- [ ] Definir modelos `gold`.
- [ ] Implementar limpieza.
- [ ] Implementar validaciones.
- [ ] Implementar agregaciones.
- [ ] Configurar Airflow.
- [ ] Automatizar ejecuciones.

**Entregable:** capas `silver` y `gold` funcionales y orquestadas.

## Fase 9 — Consulta y visualización

- [ ] Configurar DuckDB.
- [ ] Elegir entre Metabase y Streamlit.
- [ ] Crear panel de monitorización.
- [ ] Mostrar anomalías.
- [ ] Mostrar métricas.
- [ ] Mostrar estado de la DLQ.
- [ ] Mostrar información relevante del pipeline.

**Entregable:** panel funcional.

## Fase 10 — Evaluación experimental

- [ ] Diseñar matriz de experimentos.
- [ ] Definir diferentes niveles de carga.
- [ ] Ejecutar pruebas de estrés.
- [ ] Modificar parámetros de configuración seleccionados.
- [ ] Medir latencia.
- [ ] Medir throughput.
- [ ] Medir CPU.
- [ ] Medir RAM.
- [ ] Medir consumer lag.
- [ ] Medir integridad de los datos.
- [ ] Analizar resultados.
- [ ] Extraer conclusiones.

**Entregable:** informe de resultados.

## Fase 11 — Memoria y defensa

- [ ] Redactar memoria.
- [ ] Incorporar resultados experimentales.
- [ ] Documentar decisiones de arquitectura.
- [ ] Documentar problemas y soluciones.
- [ ] Preparar repositorio público.
- [ ] Preparar presentación.
- [ ] Preparar defensa.

---

# 5. Alcance

## Incluye

- Una fuente principal de datos.
- Generación de datos sintéticos.
- Captura de cambios mediante CDC.
- Procesamiento de eventos en tiempo real.
- Arquitectura por capas `bronze`, `silver` y `gold`.
- Detección de anomalías.
- Absorción de picos de carga.
- Dead-letter queue.
- Transformaciones ELT.
- Orquestación.
- Consulta.
- Visualización.
- Evaluación cuantitativa.
- Reproducibilidad mediante Docker.
- Herramientas gratuitas y open source.

## No incluye inicialmente

- Múltiples fuentes de datos complejas.
- Integraciones con sistemas externos reales.
- Modelos complejos de Machine Learning.
- Alta disponibilidad real.
- Tolerancia a fallos distribuida más allá de los escenarios definidos.
- Seguridad avanzada.
- Autenticación compleja.
- Multi-tenancy.
- Infraestructura cloud de pago.
- Datos reales de clientes.
- Integraciones con entidades bancarias reales.

Si durante el desarrollo se propone añadir una funcionalidad fuera de este alcance, debe evaluarse primero su impacto sobre el tiempo y la complejidad del TFG.

---

# 6. Decisiones tomadas

## 2026 — Ingeniería de Datos como enfoque principal

- **Elegido:** Ingeniería de Datos.
- **Alternativa:** Ciencia de Datos como eje principal.
- **Motivo:** el interés principal del autor está en la ingeniería y la construcción de sistemas de datos.
- La Ciencia de Datos puede aparecer como componente secundario, especialmente en la detección de anomalías, pero no debe dominar el proyecto.

## 2026 — La plataforma como protagonista

- **Elegido:** diseñar una plataforma de datos reutilizable.
- **Alternativa:** centrar el TFG exclusivamente en un caso de uso.
- **Motivo:** permite que la arquitectura y la ingeniería sean el núcleo del proyecto y que el dominio pueda cambiar sin rehacer la plataforma.

## 2026 — Detección de fraude en transacciones bancarias como caso de uso

- **Elegido:** detección de fraude en transacciones bancarias simuladas.
- **Motivo:** proporciona un escenario concreto sobre el que demostrar las capacidades de la plataforma de ingeniería de datos, especialmente la ingesta en tiempo real, detección de anomalías, gestión de picos de carga y tratamiento de eventos corruptos.
- El caso de uso forma parte de la definición del TFG y **no debe cambiarse por iniciativa propia**.
- Cualquier modificación del caso de uso requiere una decisión explícita del usuario.

## 2026 — Docker como entorno de despliegue

- **Elegido:** ejecutar toda la plataforma mediante Docker/Docker Compose.
- **Motivo:** evitar costes, facilitar reproducibilidad y aislar los diferentes servicios.
- No se debe introducir infraestructura cloud de pago.

## 2026 — Coste cero

- Todas las herramientas deben ser gratuitas y/o open source.
- No realizar compras de licencias.
- No introducir servicios de pago salvo autorización explícita del usuario.
- La arquitectura debe poder ejecutarse en una única máquina de desarrollo.

## 2026-09-11 — Kafka efímero entre experimentos

- **Elegido:** no dar persistencia a Kafka. Arranca vacío tras cada `docker compose down`.
- **Alternativa:** darle volúmenes con nombre, igual que a MinIO y al catálogo.
- **Motivo:** los eventos de una ejecución anterior contaminan las métricas de la siguiente (consumer lag inicial distinto de cero, throughput del primer micro-lote midiendo el vaciado de un backlog). Un Kafka vacío hace los experimentos comparables entre sí.
- **Principio que establece:** lakehouse duradero, flujo efímero.
- **Impacto:** hay que volver a registrar el conector de Debezium tras cada `down`.
- **Impacto sobre la memoria:** es una decisión metodológica sobre el diseño experimental, no un detalle de infraestructura, y como tal debería aparecer en el capítulo de evaluación.

## 2026-09-11 — Persistencia del lakehouse mediante volúmenes con nombre

- **Elegido:** declarar volúmenes con nombre para MinIO y para el catálogo Iceberg en `docker-compose.yml`.
- **Alternativas:** catálogo respaldado por PostgreSQL; o no tocar nada y usar siempre `docker compose stop`.
- **Motivo:** era la opción que resolvía el problema por completo con el menor incremento de complejidad. El análisis detallado, con la tabla comparativa de las tres alternativas y el motivo de cada descarte, está en la sección 7.
- **Impacto:** el lakehouse sobrevive a `docker compose down`, lo que era requisito para poder defender los resultados experimentales de la fase 10.

## 2026-09-11 — Cambio de decisión: una sola rama de trabajo

- **Decisión anterior:** dos ramas, `main` estable y `tfg` de desarrollo, con la instrucción de no trabajar nunca directamente sobre `main`.
- **Nueva decisión:** se trabaja **directamente sobre `main`**, que es la única rama del repositorio.
- **Motivo del cambio:** decisión explícita del usuario. El flujo de dos ramas nunca llegó a usarse: `tfg` existía solo en local, jamás se subió a GitHub, y el repositorio remoto solo ha tenido `main`. Mantener una rama que nadie ve ni revisa añadía un paso sin aportar nada.
- **Alternativas consideradas:** conservar `tfg` y subirla al remoto para trabajar con pull requests. Se descartó porque en un TFG de un solo autor el revisor es el propio usuario, que ya revisa antes de cada `push`; el punto de control real es el commit, no la rama.
- **Impacto sobre la arquitectura:** ninguno.
- **Impacto sobre la memoria:** el capítulo de metodología debe describir un flujo de rama única con commits atómicos, no un flujo de ramas paralelas.
- **Estado de `tfg`:** la rama local se conservó tras llevar su contenido a `main` mediante un avance rápido. No se ha borrado porque borrar ramas requiere confirmación explícita.

## 2026 — Pregunta de investigación

Pregunta provisional:

> ¿Cómo afectan la carga de trabajo y distintos parámetros de configuración al rendimiento de una plataforma de ingeniería de datos desplegada íntegramente con Docker?

La pregunta podrá refinarse si durante el diseño aparecen mejores variables experimentales.

---

# 7. Problemas encontrados y soluciones

Esta sección debe mantenerse actualizada durante el desarrollo.

Formato recomendado:

```markdown
### [Fecha] Título del problema

- **Síntoma:** qué ocurrió.
- **Causa:** por qué ocurrió.
- **Solución:** qué se hizo.
- **Alternativas consideradas:** otras soluciones posibles.
- **Resultado:** qué ocurrió después.
- **Impacto en el TFG:** si modificó arquitectura, planificación o alcance.
```

No registrar únicamente que "se arregló".

Los problemas técnicos relevantes son información útil para la memoria del TFG y deben documentarse.

### [2026-09-11] Los README describían un caso de uso que ya no existía

- **Síntoma:** `README.md` y `debezium/README.md` daban instrucciones que no funcionaban: hablaban de sensores IoT, de la tabla `estado_dispositivo`, del script `generate_devices.py`, del conector `iot-connector` y del tópico `iot.public.estado_dispositivo`. Ninguno de esos nombres existe ya en el código.
- **Causa:** el proyecto cambió de dominio en el commit `af4c05a`, de sensores IoT a transacciones bancarias, y se actualizó el código pero no la documentación.
- **Solución:** reescritura completa de ambos. `README.md` documenta ahora la plataforma entera y `debezium/README.md` deja de duplicarlo para centrarse solo en el CDC.
- **Resultado:** el procedimiento de registro del conector se ejecutó de principio a fin para comprobarlo: devuelve `201 Created` y deja el conector y su tarea en `RUNNING`.
- **Impacto en el TFG:** ninguno sobre la arquitectura. Recuerda que la documentación forma parte del entregable de reproducibilidad: si el README no funciona, el criterio de la sección 12 no se cumple por mucho que el código sí funcione.

### [2026-09-11] Git Bash rompe los `docker exec` con rutas absolutas

- **Síntoma:** `docker exec tfg-kafka /kafka/bin/kafka-topics.sh ...` falla con `stat C:/Program Files/Git/kafka/bin/kafka-topics.sh: no such file or directory`.
- **Causa:** Git Bash sobre Windows convierte automáticamente los argumentos que parecen rutas Unix a rutas Windows. La ruta `/kafka/bin/...` es interna del contenedor, pero Git Bash no tiene forma de saberlo y la reescribe antes de que Docker la reciba.
- **Solución:** exportar `MSYS_NO_PATHCONV=1` antes de ejecutar esos comandos, o usarlos desde PowerShell, donde el problema no existe.
- **Impacto en el TFG:** ninguno sobre la arquitectura, pero afecta a la reproducibilidad en Windows y por eso está documentado en el `README.md`.

### [2026-09-11] Las herramientas de consola de Kafka no encuentran el broker con `localhost`

- **Síntoma:** `kafka-topics.sh --bootstrap-server localhost:9092` ejecutado **dentro** del contenedor `tfg-kafka` reintenta durante un minuto con `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` y acaba en `TimeoutException`.
- **Causa:** el broker anuncia su dirección con el nombre de servicio de la red de Docker, no con `localhost`. El cliente se conecta, recibe los metadatos del clúster y a partir de ahí intenta hablar con una dirección que no resuelve.
- **Solución:** usar `--bootstrap-server kafka:9092` dentro del contenedor. Verificado: devuelve la lista de tópicos al instante.
- **Impacto en el TFG:** afecta a todos los comandos de diagnóstico de Kafka, incluidos los que harán falta en la fase de evaluación. Documentado en el `README.md` y en `debezium/README.md`.

### [2026-09-11] Replication slot huérfano del conector anterior

- **Síntoma:** `SELECT slot_name, active FROM pg_replication_slots;` devuelve dos slots: `fraude_slot` (activo, el actual) e `iot_slot` (inactivo).
- **Causa:** `iot_slot` lo creó el conector del caso de uso IoT anterior. Eliminar un conector de Debezium no elimina su replication slot en PostgreSQL.
- **Estado:** **resuelto el 2026-09-11**, con la aprobación del usuario. Antes de eliminarlo se midió cuánto WAL retenía cada slot: `iot_slot` acumulaba 1.428 kB frente a los 118 kB del slot activo, lo que confirmó el diagnóstico. Tras ejecutar `pg_drop_replication_slot` sobre él solo queda `fraude_slot`, y se comprobó que el conector sigue en `RUNNING`.
- **Por qué importa:** un slot inactivo hace que PostgreSQL conserve indefinidamente todos los segmentos del WAL que ese slot todavía no ha consumido. El disco crece sin causa aparente y, en el peor caso, la base de datos se queda sin espacio. En un TFG cuya evaluación consiste en generar mucho volumen de eventos, no es un detalle menor.
- **Solución propuesta:** `SELECT pg_drop_replication_slot('iot_slot');`, solo tras confirmación.

### [2026-09-11] El lakehouse no sobrevive a un `docker compose down`

- **Síntoma:** ni el servicio `minio` ni el servicio `rest` declaran `volumes:` en el `docker-compose.yml`.
- **Diagnóstico real (corregido tras inspeccionar los contenedores):** son dos problemas distintos, y el grave es el segundo.
  - **MinIO sí tiene volumen, pero anónimo.** La imagen `minio/minio` declara `VOLUME /data` en su Dockerfile, así que Docker le crea un volumen anónimo con nombre aleatorio (verificado: `2ade2b47…`, con 2,3 MB de datos). Sobrevive a `restart` y a `stop`/`start`. Lo que no sobrevive es un `docker compose down`: el contenedor se elimina y el siguiente `up` crea uno nuevo con un volumen anónimo **nuevo y vacío**. Los datos no se borran, quedan varados en un volumen colgante — recuperables a mano, pero en la práctica perdidos, y acumulando basura en disco. Un `docker volume prune` sí los borra de verdad.
  - **El catálogo Iceberg no tiene ningún volumen.** `tfg-iceberg-rest` arranca con `CATALOG_URI=jdbc:sqlite:/tmp/iceberg_catalog.db`, un fichero SQLite dentro de la capa de escritura del contenedor (verificado: su lista de montajes está vacía). Ese fichero es el que traduce `demo.bronze.eventos_cuenta` a la ruta de sus ficheros de metadatos. Si desaparece, los datos siguen en MinIO pero dejan de ser alcanzables como tabla.
- **La trampa, que es lo peor del asunto:** los checkpoints de Spark **sí** persisten, porque viven en `/home/iceberg/warehouse/_checkpoints`, que es un *bind mount* a `./warehouse` del repositorio (verificado: 227 kB en el anfitrión). Tras un `down` + `up` queda un estado incoherente: el catálogo está vacío y las tablas ya no existen, pero Spark reanuda desde los offsets de Kafka que ya tenía guardados y **no vuelve a procesar esos eventos**. Las tablas se recrean vacías y pierden para siempre todo lo anterior, sin ningún error visible.
- **Alternativas consideradas:**

  | Opción | En qué consiste | Ventajas | Inconvenientes |
  |---|---|---|---|
  | **A. Volúmenes con nombre** | Declarar en `docker-compose.yml` un volumen para `/data` de MinIO y otro para el fichero del catálogo | Cambio mínimo, reversible, sin servicios nuevos; deja explícito en el compose qué datos son duraderos | No convierte el catálogo en uno "de producción": sigue siendo un SQLite |
  | **B. Catálogo sobre PostgreSQL** | Sustituir el SQLite por una base de datos en el `tfg-postgres` existente | Más robusto y más parecido a un despliegue real; un único motor que respaldar | Más configuración; acopla el catálogo a la base de datos que además hace de origen del caso de uso, lo que enturbia el papel de cada componente en la arquitectura |
  | **C. No tocar nada** | Usar siempre `docker compose stop` en lugar de `down` | Coste cero | Depende de no equivocarse nunca; un `down` accidental destruye un experimento en curso sin avisar |

- **Solución elegida: la A**, aplicada el 2026-09-11 con la aprobación del usuario. El criterio fue el de la sección 20: resuelve el problema por completo con el menor incremento de complejidad, sin añadir servicios ni dependencias. La B se descartó porque su ventaja real —robustez del motor de catálogo— no aporta nada a la pregunta de investigación, que va sobre rendimiento, y sí añadía acoplamiento entre dos componentes que en la arquitectura cumplen papeles distintos. La C se descartó porque convierte un problema técnico en una regla que hay que recordar, y las reglas que hay que recordar se incumplen.
- **Cómo se aplicó:** volúmenes `miniodata` (montado en `/data`) e `icebergcatalog` (montado en `/home/iceberg`), más `CATALOG_URI: jdbc:sqlite:/home/iceberg/iceberg_catalog.db`. El montaje se hizo sobre `/home/iceberg` y no sobre un subdirectorio nuevo porque ese directorio ya existe en la imagen y pertenece al usuario `iceberg` (uid 1000) con el que corre el proceso: Docker conserva esa propiedad al inicializar el volumen, mientras que un subdirectorio inexistente lo habría creado como `root` y el proceso no habría podido escribir.
- **Resultado, verificado:** se reconstruyeron las 2.511 filas de `bronze`, se ejecutó `docker compose down` y `docker compose up -d`, y la tabla siguió ahí con las mismas 2.511 filas y 420 anomalías. Antes del cambio ese mismo `down` la habría vaciado en silencio.
- **Impacto en el TFG:** elimina un riesgo directo sobre la reproducibilidad de la fase 10 y sobre las métricas de integridad de la sección 14.3, que un pipeline capaz de perder eventos sin avisar habría contaminado.

### [2026-09-11] El consumer lag vale 0 por construcción, no porque el sistema vaya sobrado

- **Síntoma:** en la primera prueba del modo de pico de carga, la ráfaga se ve perfectamente en las métricas —las filas por micro-lote pasan de 10 a 6.974 y el throughput de 10 a 7.369 filas/s— pero el `lag_maximo` permanece en **0 en todos los micro-lotes**, incluso durante el pico.
- **Causa:** el lag se toma de `maxOffsetsBehindLatest`, que la fuente de Kafka calcula como la diferencia entre el último offset disponible y el último que el micro-lote ha leído. Como los jobs **no tienen configurado `maxOffsetsPerTrigger`**, Spark consume en cada micro-lote *todo* lo que hay disponible, así que ese valor es cero por definición. No mide que el sistema vaya holgado: mide una resta que siempre da cero.
- **Corrección:** esto invalida lo que se había documentado antes, que presentaba el consumer lag como la señal principal de las pruebas de pico. **Con la configuración actual, el lag no sirve como indicador de saturación.**
- **Qué usar en su lugar, mientras tanto:** `filas_entrada` por micro-lote, que sí refleja cuánto se ha acumulado entre disparos, y sobre todo la comparación entre `duracion_total_ms` y el intervalo del *trigger*. Mientras un micro-lote tarde menos que su intervalo, el job va al día; cuando lo supere de forma sostenida, se estará quedando atrás. En la prueba, los micro-lotes tardaron entre 927 y 1.074 ms frente a un trigger de 10 s: un margen de casi diez veces.
- **Cómo hacer que el lag vuelva a ser informativo:** fijar `maxOffsetsPerTrigger` para acotar cuántos eventos entra en cada micro-lote. Entonces un pico que supere ese límite deja eventos esperando y el lag pasa a medir exactamente eso. **Y es, además, un excelente parámetro de configuración que estudiar** en la matriz de experimentos de la fase 10: relaciona directamente tamaño de micro-lote con latencia y con capacidad de absorción, que es el núcleo de la pregunta de investigación.
- **Impacto en el TFG:** afecta al diseño de los experimentos de la fase 10 y a cómo se interpretan sus resultados. Un lag de cero no se puede presentar como prueba de que la plataforma aguanta si es cero por construcción.

### [2026-09-11] El generador es el cuello de botella, no la plataforma

- **Síntoma:** con `PICO_INTERVALO_SEGUNDOS=0`, es decir, generando tan rápido como puede, el generador alcanza unos **692 eventos/s**. Con `PICO_INTERVALO_SEGUNDOS=0.005`, que pediría 200 eventos/s, alcanza solo 140.
- **Causa:** el generador es un único proceso de Python que hace un `INSERT ... ON CONFLICT` síncrono por transacción. Cada una es un viaje de ida y vuelta a PostgreSQL, y ese coste marca el techo. El `time.sleep()` tampoco es preciso por debajo de unos pocos milisegundos, de ahí que 0,005 s no dé los 200 eventos/s teóricos.
- **Consecuencia:** Spark procesó esos 692 eventos/s en micro-lotes de aproximadamente un segundo. **La plataforma no llegó a despeinarse, así que este pico no es todavía una prueba de estrés real.**
- **Opciones para subir el techo, si hiciera falta:** agrupar varias transacciones por transacción de base de datos (`execute_batch` de psycopg2), lanzar varios procesos generadores en paralelo, o aceptar el techo actual y estrechar artificialmente el pipeline con `maxOffsetsPerTrigger`.
- **Recomendación:** empezar por lo segundo. Estrechar el pipeline es más barato que acelerar el generador, y además convierte el estrangulamiento en un parámetro de configuración medible, que es justo lo que la pregunta de investigación quiere estudiar. Acelerar el generador solo es necesario si se quiere conocer el límite absoluto de la plataforma.
- **Impacto en el TFG:** es un dato honesto y necesario para la memoria. Sin él, un lector podría interpretar «el sistema absorbió el pico sin degradarse» como una medida de la capacidad de la plataforma, cuando en realidad mide la capacidad del generador.

### [2026-09-11] Kafka y el conector tampoco sobreviven a un `docker compose down`

- **Síntoma:** detectado al verificar la solución del problema anterior. Tras `docker compose down` y `up -d`, el tópico `fraude.public.estado_cuenta` había desaparecido y `curl localhost:8083/connectors` devolvía `[]`: el conector ya no estaba registrado.
- **Causa:** es el mismo mecanismo. Las imágenes de Kafka declaran `VOLUME` sobre `/kafka/data`, `/kafka/logs` y `/kafka/config`, así que Docker crea volúmenes anónimos que no se reasignan al recrear el contenedor. La configuración del conector vive en el tópico `connect_configs`, que se pierde con ellos.
- **Consecuencia práctica:** hay que volver a registrar el conector después de cada `docker compose down`. Ya está recogido como paso del procedimiento en el `README.md` y en la sección 13.
- **Resolución: se deja así a propósito.** Decisión explícita del usuario del 2026-09-11: Kafka debe arrancar limpio en cada experimento. No se le añaden volúmenes con nombre.
- **Motivo:** un tópico que conserva los eventos de la ejecución anterior contamina las medidas. El consumer lag arranca distinto de cero, el throughput del primer micro-lote mide el vaciado de un backlog en vez del régimen normal, y hay que separar el transitorio de arranque en frío del estado estacionario. Ese efecto ya se observó en la prueba del 2026-09-11: latencia máxima de 87 s frente a una media de 10,89 s, debida exclusivamente al backlog acumulado. Partir de un Kafka vacío elimina esa fuente de ruido y hace los experimentos comparables entre sí.
- **Principio de diseño que esto establece: lakehouse duradero, flujo efímero.** Los resultados de un experimento tienen que poder consultarse después; los eventos que lo produjeron, no. Es una asimetría deliberada y conviene presentarla así en la memoria, como decisión razonada, en lugar de como un descuido de configuración.
- **Consecuencia operativa:** tras cada `docker compose down` hay que volver a registrar el conector de Debezium, porque su configuración vive en el tópico `connect_configs`. Está recogido como paso del procedimiento en el `README.md` y en la sección 13.
- **Cuándo habría que reconsiderarlo:** si en algún momento hiciera falta reproducir un experimento a partir de exactamente los mismos eventos, en lugar de regenerarlos. En ese caso habría que dar volumen con nombre a Kafka y asumir el ruido que introduce.

---

# 8. Pruebas y resultados

Los resultados deben ser cuantitativos siempre que sea posible.

No escribir únicamente:

> "El pipeline funciona correctamente."

Registrar datos como:

- Eventos/segundo.
- Latencia media.
- Latencia máxima.
- Consumer lag.
- CPU.
- RAM.
- Eventos procesados.
- Eventos perdidos.
- Eventos enviados a DLQ.
- Eventos recuperados.
- Precisión de detección.
- Exhaustividad/recall.
- Verdaderos positivos.
- Falsos positivos.
- Falsos negativos.

Formato recomendado:

```markdown
### [Fecha] Prueba: descripción

- **Objetivo:**
- **Configuración:**
- **Carga:**
- **Parámetros modificados:**
- **Métricas obtenidas:**
- **Resultado:**
- **Conclusión:**
```

Los datos utilizados en la memoria final deben proceder de pruebas reproducibles siempre que sea posible.

### [2026-09-11] Prueba: primer pico de carga

Primera ejecución del modo de pico de carga (tarea 30), con el job de `bronze` corriendo e instrumentado.

- **Objetivo:** comprobar que el modo de pico genera una ráfaga real, que atraviesa el pipeline completo y que la instrumentación la captura.
- **Configuración:** `SEMILLA=tfg2026`, 20 cuentas, ritmo normal de 1 evento/s, duración total 150 s. Una ráfaga de 30 s comenzando en t+45 s, con `PICO_INTERVALO_SEGUNDOS=0` (máximo ritmo posible) sobre 5 cuentas. Trigger de `bronze` a 10 s.
- **Carga generada:** 20.890 eventos en 150,3 s. De ellos, **20.770 dentro de la ráfaga de 30 s**.

| Régimen | Ritmo de generación | Filas por micro-lote | Throughput de Spark | Duración del micro-lote |
|---|---|---|---|---|
| Normal | 1 evento/s | ~10 | ~10 filas/s | ~1.000 ms |
| Pico | **692 eventos/s** | hasta **6.974** | hasta **7.369 filas/s** | 927–1.074 ms |

- **Consumer lag:** 0 en los 15 micro-lotes, también durante el pico. Ver la sección 7: es cero por construcción, no por holgura.
- **Comprobación de extremo a extremo:** las transacciones de card testing llegaron a `bronze`. Consultando las compras online de menos de 3 €, aparecen **22.828 filas con un importe medio de 1,75 €**, coherente con el patrón inyectado.

**Conclusiones:**

1. **El modo de pico funciona y es claramente observable.** Un salto de 1 a 692 eventos/s se traduce en micro-lotes que pasan de 10 a casi 7.000 filas. La ráfaga queda perfectamente delimitada en el resumen JSON del generador, con sus marcas temporales absolutas, lo que permite superponerla sobre las gráficas de métricas.
2. **La plataforma absorbió el pico sin degradarse.** La duración del micro-lote apenas se movió: entre 927 y 1.074 ms frente a un *trigger* de 10 s, es decir, casi un factor diez de margen. Procesar 6.974 filas le costó lo mismo que procesar 10, lo que sugiere que a estos volúmenes domina el coste fijo de cada micro-lote y no el de los datos.
3. **Este pico todavía no es una prueba de estrés.** El límite lo pone el generador, no la plataforma (sección 7). Para provocar degradación observable hay que estrechar el pipeline con `maxOffsetsPerTrigger` o acelerar el generador.
4. **El detector de anomalías es ciego al card testing, como se esperaba.** Las 20.770 transacciones de la ráfaga llevan importes de entre 0,50 y 3 €: lejísimos de superar un z-score de 3 por arriba. Es una limitación real del método estadístico elegido y conviene presentarla como tal en la memoria: el z-score sobre el importe detecta el fraude por cuantía, no el fraude por frecuencia. Detectar el card testing exigiría una métrica sobre el ritmo de transacciones por cuenta y unidad de tiempo, no sobre su importe.

### [2026-09-11] Prueba: primera medición de detección, throughput y latencia

Primera vez que el pipeline produce números y no solo trazas por consola. Es una prueba de verificación de la instrumentación, no un experimento formal: sirve para comprobar que todo se mide, y de paso deja una línea base.

- **Objetivo:** comprobar que la etiqueta de verdad permite calcular precisión y exhaustividad, y que el listener de métricas y el muestreo de recursos registran datos utilizables.
- **Configuración:** entorno completo en Docker. Trigger de `bronze` a 10 s, trigger del detector a 15 s, `VENTANA_LECTURAS=20`, `Z_SCORE_UMBRAL=3.0`. Tópico Kafka con 1 partición.
- **Carga:** dos tramos. Primero 10 cuentas con un evento cada 0,2 s y `PROB_ANOMALIA=0.3`; después 20 cuentas con un evento cada 0,05 s y `PROB_ANOMALIA=0.15`.

**Métricas de detección** (sobre las 242 primeras transacciones, con `PROB_ANOMALIA=0.3`):

| Métrica | Valor |
|---|---|
| Transacciones evaluadas | 242 |
| Anomalías inyectadas | 75 |
| Verdaderos positivos | 7 |
| Falsos positivos | 0 |
| Falsos negativos | 68 |
| Precisión | 100 % |
| Exhaustividad (recall) | 9,3 % |

Desglose por tipo de anomalía inyectada: 3 detectadas de 32 de tipo `importe`, 4 de 43 de tipo `importe_ubicacion`.

**Métricas de rendimiento** (13 micro-lotes del job de `bronze`):

| Métrica | Valor |
|---|---|
| Throughput | entre 114 y 339 filas/s |
| Duración del micro-lote | entre 566 y 3.929 ms |
| Consumer lag máximo | 0 |
| Latencia media de extremo a extremo | 10,89 s sobre 2.511 eventos |
| Latencia máxima | 87 s |
| Pico de CPU de `tfg-spark` | 744 % (más de 7 núcleos) |
| Memoria de `tfg-spark` | 4.233 MiB de 7.900 MiB disponibles (53 %) |

**Conclusiones:**

1. **La exhaustividad es malísima y la causa es metodológica, no un fallo del código.** Con un 30 % de transacciones anómalas, las propias anomalías dominan la media y la desviación típica que el detector calcula sobre las últimas 20 transacciones de cada cuenta. El umbral de z-score 3,0 se vuelve casi inalcanzable porque la desviación está inflada por lo que se pretende detectar. Es un caso de libro de contaminación de la línea base, y respalda la elección de la propuesta de inyectar solo un 3-5 % de anomalías. **Hay que repetir la medición con `PROB_ANOMALIA` en su valor por defecto de 0,04 antes de sacar cualquier conclusión sobre la calidad del detector.**
2. **La precisión del 100 % con 0 falsos positivos no es tan buena noticia como parece:** es coherente con un umbral demasiado conservador, que solo dispara ante lo extremadamente evidente. Precisión y exhaustividad hay que leerlas juntas.
3. **La latencia media de 10,89 s es esencialmente el trigger de 10 s**, no un límite del sistema: un evento que llega justo después de un micro-lote espera al siguiente. Confirma que el tamaño del trigger es uno de los parámetros de configuración con efecto más directo sobre la latencia, y por tanto un buen candidato para la matriz de experimentos.
4. **La latencia máxima de 87 s corresponde al arranque en frío**, cuando el job consume de golpe el backlog acumulado con `startingOffsets: earliest`. Al analizar los resultados hay que separar el transitorio de arranque del régimen estacionario, o las medias salen distorsionadas.
5. **El lag se mantuvo en 0 todo el tiempo:** con 20 eventos/s el sistema va sobrado, así que esta carga no sirve como prueba de estrés. Habrá que subirla bastante hasta ver degradación, tal como anticipaba la sección 16 de riesgos.
6. **`tfg-spark` consume más de la mitad de la memoria disponible para Docker.** Es el componente que marcará el techo de los experimentos en esta máquina, y conviene tenerlo presente al diseñar la matriz de carga.

---

# 9. Convenciones de código

- Lenguaje principal: Python para el generador y componentes auxiliares.
- Spark Structured Streaming para procesamiento de streaming.
- SQL para PostgreSQL y consultas.
- dbt para transformaciones ELT.
- Comentarios y docstrings en español, salvo que exista una razón clara para utilizar inglés.
- Nombres de variables, funciones, clases y archivos pueden estar en inglés cuando sea más natural y siga las convenciones del ecosistema.
- Seguir primero el estilo existente del repositorio antes de introducir nuevas convenciones.
- Evitar duplicación innecesaria.
- No introducir dependencias innecesarias.
- Mantener los componentes desacoplados.
- Las configuraciones deben ser parametrizables cuando tenga sentido.
- Las credenciales y secretos nunca deben estar hardcodeados.
- Utilizar `.env` para configuración sensible.
- `.env` debe estar incluido en `.gitignore`.
- Si se añade una dependencia nueva, actualizar el archivo de dependencias correspondiente en el mismo commit.
- No modificar versiones de dependencias sin justificarlo.

---

# 10. Flujo de trabajo con Git

Reglas fijas para cómo Claude Code debe utilizar Git:

- Commits frecuentes y atómicos.
- Un commit debe representar un cambio lógico completo y funcional.
- No crear un commit gigante al final de una sesión.
- Evitar commits de código roto salvo que sea explícitamente un WIP.
- Utilizar Conventional Commits en español.

Ejemplos:

```text
feat: añade generador de transacciones bancarias
feat: implementa captura CDC con Debezium
feat: añade procesamiento streaming con Spark
fix: corrige configuración de Kafka
fix: evita pérdida de eventos en la DLQ
test: añade prueba de throughput del pipeline
docs: documenta decisión sobre almacenamiento
```

## Protocolo de entrega de cambios (checkpoints)

Esta es una norma fija de trabajo, establecida por el usuario, y se aplica a
**todos** los cambios del proyecto.

El trabajo avanza por **checkpoints pequeños**. Cada cambio mínimamente
completo se cierra antes de empezar el siguiente, en lugar de acumular muchas
modificaciones y entregarlas todas juntas al final. Un checkpoint es una
unidad que se sostiene por sí sola: se puede revisar, probar y revertir sin
arrastrar nada más.

En cada checkpoint hay que hacer tres cosas, siempre en este orden:

1. **Verificar el cambio.** Siempre que el entorno lo permita, ejecutarlo de
   verdad y comprobar el resultado, en vez de darlo por bueno porque el código
   exista. Es la misma regla de la sección 19 sobre el estado de las fases.
2. **Hacer un commit local**, con Conventional Commits en español y un cuerpo
   que explique qué se ha hecho y por qué. **Nunca hacer `git push`**: el
   usuario revisa el código y publica los cambios él mismo. Esa separación es
   deliberada, y el commit es precisamente el punto de control que le permite
   revisar un cambio aislado antes de publicarlo.
3. **Devolverle la explicación por escrito** en la respuesta, para que pueda
   leerla sin abrir el repositorio.

### Estructura de la explicación

Cada cambio se explica por completo, siguiendo esta estructura:

- **Qué se ha hecho.** El cambio concreto, en lenguaje claro.
- **Por qué.** El problema que resuelve o la necesidad que cubre. Esta parte es
  la más importante: es la que acaba convirtiéndose en texto de la memoria.
- **Qué alternativas se consideraron** y por qué se descartaron, cuando había
  una decisión real que tomar.
- **Cómo se ha verificado**, con los datos concretos obtenidos.
- **Qué impacto tiene** sobre la arquitectura, el alcance o las fases.
- **Qué queda pendiente** o qué decisión necesita el usuario.

No basta con dejar la explicación en el mensaje del commit o en este archivo:
el usuario quiere leerla en la respuesta. El motivo es doble. Por un lado, es
la forma en que sigue y valida lo que se está construyendo sin tener que
reconstruirlo leyendo diffs. Por otro, esas explicaciones son materia prima
directa para la memoria académica del TFG, según la sección 17.

## Ramas

El repositorio utiliza **una única rama: `main`**. Es la única que existe en GitHub y sobre la que se trabaja directamente.

**Decisión del usuario del 2026-09-11**, que sustituye al flujo anterior de dos ramas (`main` + `tfg`). Ese flujo nunca llegó a usarse: `tfg` era local y nunca se subió. Ver la sección 6.

No hay que crear ramas por fase ni por funcionalidad. Si en algún momento hiciera falta una rama adicional, debe consultarse antes con el usuario.

La rama `tfg` sigue existiendo en local, apuntando al mismo commit que `main`. No se ha borrado porque borrar ramas requiere confirmación explícita.

## Publicación

El usuario hace el `push` manualmente, después de revisar el código.

Claude Code:

- **NO debe hacer `git push` nunca.**
- NO debe crear pull requests automáticamente.
- Debe dejar `main` limpia, probada y documentada, con commits atómicos que el usuario pueda revisar uno a uno antes de publicar.

## Operaciones destructivas

Nunca ejecutar sin confirmación explícita:

- `rm -rf`
- `DROP TABLE`
- `docker volume prune`
- `docker compose down -v`
- Borrado masivo de datos.
- Borrado de ramas.
- Force push.
- Reescritura del historial.

No utilizar:

- `git push --force`
- rebase destructivo

sobre historial compartido salvo petición explícita del usuario.

---

# 11. Cuándo pedir confirmación antes de actuar

Pedir confirmación antes de:

1. Ejecutar comandos destructivos.
2. Cambiar significativamente la arquitectura.
3. Sustituir una tecnología principal.
4. Modificar el modelo de datos de manera importante.
5. Añadir una tecnología que incremente considerablemente la complejidad.
6. Instalar dependencias nuevas de forma relevante.
7. Cambiar versiones importantes.
8. Modificar configuraciones sensibles.
9. Modificar `.gitignore`.
10. Realizar merge.
11. Borrar ramas.
12. Hacer force push.
13. Introducir servicios de pago.
14. Ampliar significativamente el alcance del TFG.

Para cambios de arquitectura importantes, utilizar primero un modo de planificación y explicar:

- Qué se quiere cambiar.
- Por qué.
- Qué alternativas existen.
- Ventajas.
- Inconvenientes.
- Impacto sobre el TFG.
- Impacto sobre el cronograma.

---

# 12. Entorno y ejecución

La plataforma debe ejecutarse localmente.

## Orquestación

Inicialmente:

```bash
docker compose up -d
```

Para detener:

```bash
docker compose down
```

No utilizar:

```bash
docker compose down -v
```

sin confirmación, porque elimina los volúmenes.

## Principio de reproducibilidad

Una persona externa debería poder:

1. Clonar el repositorio.
2. Configurar las variables necesarias.
3. Ejecutar Docker Compose.
4. Levantar la plataforma.
5. Ejecutar el generador.
6. Reproducir el pipeline.
7. Ejecutar los experimentos documentados.

Las instrucciones necesarias deben mantenerse actualizadas en el `README.md`.

---

# 13. Guía de comandos y procedimientos

Esta sección funciona como **guía operativa viva del proyecto**.

Claude Code debe actualizarla cuando se descubra o establezca un comando, procedimiento o secuencia de comandos relevante para:

- Levantar el proyecto.
- Detener el proyecto.
- Reiniciar servicios.
- Comprobar el estado de los servicios.
- Consultar logs.
- Ejecutar el generador.
- Comprobar PostgreSQL.
- Comprobar Debezium.
- Comprobar Kafka.
- Ejecutar Spark.
- Consultar MinIO/Iceberg.
- Ejecutar dbt.
- Ejecutar Airflow.
- Consultar DuckDB.
- Ejecutar el dashboard.
- Ejecutar pruebas.
- Ejecutar experimentos.
- Depurar problemas habituales.
- Recuperar el entorno después de un fallo.

## Regla fundamental

**No documentar comandos hipotéticos como si estuvieran comprobados.**

Cuando sea posible, un procedimiento documentado debe haber sido ejecutado y verificado previamente.

Cada procedimiento debe indicar:

- Para qué sirve.
- Comando o comandos necesarios.
- Orden de ejecución si hay varios.
- Resultado esperado.
- Precauciones importantes.
- Si requiere que determinados servicios estén activos.

Ejemplo:

### Levantar todo el entorno

**Objetivo:** iniciar todos los servicios definidos en Docker Compose.

```bash
docker compose up -d
```

**Comprobación:**

```bash
docker compose ps
```

**Resultado esperado:** los servicios necesarios aparecen en estado operativo.

### Detener el entorno

**Objetivo:** detener los contenedores sin eliminar los volúmenes.

```bash
docker compose down
```

**Precaución:** no utilizar `docker compose down -v` salvo que se quiera eliminar también la información almacenada en los volúmenes y el usuario lo haya confirmado.

### Consultar logs de un servicio

```bash
docker compose logs -f <servicio>
```

**Uso:** diagnóstico y seguimiento de errores de un servicio concreto.

### Levantar el pipeline completo (procedimiento verificado el 2026-09-11)

**Objetivo:** poner en marcha todo el flujo, de generador a capa silver.

**Requiere:** Docker en marcha y el entorno virtual de `generator/` creado.

Orden de ejecución, cada bloque en su propia terminal:

```bash
# 1. Servicios
docker compose up -d

# 2. Conector CDC (esperar 20-30 s a que tfg-connect arranque del todo)
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json"   localhost:8083/connectors/ -d @debezium/register-connector.json
curl -s localhost:8083/connectors/fraude-connector/status

# 3. Generador
python generator/generate_transactions.py

# 4. Capa bronze
docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/read_kafka_to_bronze.py

# 5. Detección de anomalías
docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/detectar_anomalias.py
```

**Resultado esperado:** el registro del conector devuelve `201 Created` y, al consultar su estado, tanto el conector como su tarea aparecen en `RUNNING`. Los dos jobs de Spark imprimen `[metricas] <job>: escribiendo en ...` al arrancar.

**Precaución:** el paso 5 necesita que el paso 4 haya escrito ya algo en `bronze`, porque el detector calcula la media y la desviación a partir de ese histórico. En un entorno recién levantado, los primeros micro-lotes del detector no marcarán nada.

### Kafka: usar `kafka:9092`, nunca `localhost:9092`

Dentro del contenedor `tfg-kafka`, las herramientas de consola deben apuntar al nombre de servicio:

```bash
docker exec tfg-kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Con `localhost:9092` el cliente reintenta durante un minuto y acaba en `TimeoutException`. El motivo está explicado en la sección 7.

### Git Bash en Windows: `MSYS_NO_PATHCONV=1`

Antes de cualquier `docker exec` que invoque una ruta absoluta dentro del contenedor:

```bash
export MSYS_NO_PATHCONV=1
```

Sin esto, Git Bash reescribe `/kafka/bin/...` como ruta de Windows y el comando falla. En PowerShell no hace falta.

### Evaluar la detección

**Objetivo:** obtener la matriz de confusión. **Requiere** datos en `demo.silver.transacciones_validadas`.

```bash
docker exec -it tfg-spark spark-sql -e "
SELECT
  count(*)                                                                      AS total,
  sum(CASE WHEN es_anomalia     AND es_anomalia_generada     THEN 1 ELSE 0 END) AS verdaderos_positivos,
  sum(CASE WHEN es_anomalia     AND NOT es_anomalia_generada THEN 1 ELSE 0 END) AS falsos_positivos,
  sum(CASE WHEN NOT es_anomalia AND es_anomalia_generada     THEN 1 ELSE 0 END) AS falsos_negativos
FROM demo.silver.transacciones_validadas;"
```

### Medir latencia de extremo a extremo

```bash
docker exec -it tfg-spark spark-sql -e "
SELECT
  round(avg(unix_timestamp(fecha_ingesta) - unix_timestamp(to_timestamp(fecha_actualizacion))), 2) AS latencia_media_s,
  round(max(unix_timestamp(fecha_ingesta) - unix_timestamp(to_timestamp(fecha_actualizacion))), 2) AS latencia_maxima_s,
  count(*) AS eventos
FROM demo.bronze.eventos_cuenta;"
```

**Precaución al interpretarlo:** el resultado incluye la espera del trigger (10 s en `bronze`) y, si el job arrancó con backlog acumulado, también el transitorio de arranque en frío. Conviene separar ambos efectos antes de dar la media por buena.

### Recoger CPU y memoria durante un experimento

```bash
python scripts/recolectar_recursos.py --intervalo 5 --duracion 300 --etiqueta pico_carga
```

Escribe `warehouse/_metricas/pico_carga_<marca>.csv`, una fila por contenedor y muestra. Con `--duracion 0` muestrea hasta `Ctrl+C`.

**Precaución:** `docker stats` consume CPU. Por debajo de uno o dos segundos de intervalo, la medición contamina lo medido.

### Comprobar los replication slots de PostgreSQL

```bash
docker exec -it tfg-postgres psql -U tfg -d iot   -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"
```

**Para qué:** un slot inactivo retiene segmentos del WAL indefinidamente y el disco crece sin causa aparente. Conviene revisarlo si PostgreSQL empieza a ocupar más de lo esperado, y siempre después de eliminar un conector.

## Organización

A medida que avance el proyecto, organizar esta sección por herramienta:

```text
### Docker
### PostgreSQL
### Debezium
### Kafka / Redpanda
### Spark
### MinIO / Iceberg
### dbt
### Airflow
### DuckDB
### Metabase / Streamlit
### Generador de datos
### Pruebas
### Experimentos
### Diagnóstico y recuperación
```

La guía debe contener **conocimiento operativo reutilizable**, no un historial de cada comando ejecutado durante una sesión.

No añadir una entrada simplemente porque se haya ejecutado un comando rutinario.

---

# 14. Evaluación experimental

La evaluación es una parte fundamental del TFG.

No basta con demostrar que el pipeline funciona.

Hay que estudiar cómo se comporta.

## 14.1 Variables de carga

Se podrán utilizar diferentes niveles de:

- Número de máquinas.
- Eventos por segundo.
- Duración del experimento.
- Intensidad de los picos.

## 14.2 Parámetros de configuración

Se seleccionarán durante la fase de diseño aquellos parámetros que tengan sentido estudiar.

Ejemplos potenciales:

- Número de particiones de Kafka.
- Paralelismo de Spark.
- Tamaño de microbatch.
- Tasa de generación.
- Configuración de consumidores.
- Otros parámetros relevantes.

No asumir que todos ellos deben estudiarse: primero comprobar su viabilidad y relevancia.

## 14.3 Métricas

Las métricas principales serán:

### Rendimiento

- Throughput.
- Eventos procesados por segundo.

### Latencia

- Tiempo desde generación/captura hasta procesamiento/persistencia.

### Recursos

- CPU.
- Memoria RAM.

### Kafka

- Consumer lag.
- Eventos pendientes.
- Capacidad de absorción del buffer.

### Integridad

- Eventos generados.
- Eventos procesados.
- Eventos perdidos.
- Eventos enviados a DLQ.
- Eventos correctamente recuperados.

### Detección

- Precisión.
- Exhaustividad/recall.
- Verdaderos positivos.
- Falsos positivos.
- Falsos negativos.

---

# 15. Resultados esperados

Se espera obtener:

1. Una plataforma funcional de ingeniería de datos para procesamiento de eventos en tiempo real.
2. Un pipeline CDC de extremo a extremo.
3. Un mecanismo de detección de anomalías integrado en el procesamiento.
4. Un sistema capaz de absorber picos de carga.
5. Una dead-letter queue capaz de aislar eventos corruptos sin detener el pipeline principal.
6. Capas `bronze`, `silver` y `gold`.
7. Transformaciones ELT automatizadas.
8. Un sistema de consulta y visualización.
9. Métricas cuantitativas de latencia y throughput.
10. Métricas de consumo de recursos.
11. Resultados experimentales sobre diferentes cargas.
12. Resultados experimentales sobre distintos parámetros de configuración.
13. Métricas de integridad de los datos.
14. Una arquitectura reproducible mediante Docker.
15. Un repositorio público con instrucciones para reproducir el proyecto.
16. La memoria completa del TFG y el material para su defensa.

---

# 16. Riesgos y mitigación

| Riesgo | Mitigación |
|---|---|
| Complejidad de integrar varias tecnologías nuevas simultáneamente | Construir primero una versión mínima de extremo a extremo y añadir componentes incrementalmente |
| Recursos limitados de la máquina de desarrollo | Reducir configuraciones, limitar recursos y utilizar alternativas más ligeras cuando sea necesario |
| Spark, Kafka y MinIO consumen demasiada memoria | Evaluar Redpanda u otras alternativas ligeras si fuese necesario |
| Curva de aprendizaje de Debezium, Iceberg, dbt, Airflow y Spark | Aprender cada tecnología de forma incremental y priorizar primero el funcionamiento del pipeline mínimo |
| Scope creep | Revisar el alcance periódicamente y no añadir funcionalidades sin valorar su impacto |
| Experimentos demasiado complejos | Definir una matriz de experimentación limitada y reproducible |
| Los picos de carga no producen degradación observable | Aumentar progresivamente la intensidad hasta alcanzar la capacidad de la máquina |
| La DLQ acumula demasiados eventos | Limitar el porcentaje de eventos corruptos y definir mecanismos claros de revisión |
| Parámetros de anomalía poco realistas | Empezar con métodos estadísticos simples y justificar los parámetros mediante los datos generados |
| El sistema completo consume demasiados recursos | Ejecutar pruebas por fases, limitar recursos de Docker y simplificar componentes no esenciales |
| Una tecnología resulta incompatible con otra | Evaluar alternativas open source antes de modificar significativamente la arquitectura |
| El proyecto crece demasiado | Priorizar siempre el pipeline funcional y la evaluación experimental sobre funcionalidades secundarias |

---

# 17. Relación con la memoria académica

Todas las decisiones relevantes del desarrollo deben poder convertirse posteriormente en contenido de la memoria.

Claude Code debe conservar información sobre:

- Por qué se eligió cada tecnología.
- Qué alternativas se consideraron.
- Qué problemas aparecieron.
- Cómo se resolvieron.
- Qué experimentos se realizaron.
- Qué configuración se utilizó.
- Qué resultados se obtuvieron.
- Qué conclusiones se extrajeron.

No eliminar información histórica importante simplemente porque una decisión posterior la haya sustituido.

Si una decisión cambia, documentar:

```markdown
### [Fecha] Cambio de decisión

- **Decisión anterior:**
- **Nueva decisión:**
- **Motivo del cambio:**
- **Alternativas consideradas:**
- **Impacto sobre la arquitectura:**
- **Impacto sobre la memoria:**
```

Esto permitirá justificar posteriormente la evolución del proyecto.

---

# 18. Documento de propuesta

El archivo:

```text
propuesta_tfg_fraude_bancario.md
```

contiene la propuesta inicial del TFG y debe considerarse una fuente fundamental de contexto para el proyecto.

La propuesta contiene la definición inicial del proyecto y recoge:

- Introducción y motivación.
- Objetivos.
- Alcance.
- Caso de uso.
- Arquitectura y metodología técnica.
- Stack tecnológico.
- Metodología de trabajo.
- Plan de trabajo y cronograma.
- Resultados esperados.
- Riesgos y mitigación.
- Referencias preliminares.

La documentación de la propuesta se considera **correcta y válida como base del proyecto**.

Durante el desarrollo pueden producirse cambios en decisiones técnicas, arquitectura, tecnologías, metodología o planificación. Esto es normal y forma parte de la evolución del TFG. Cuando una decisión cambie, debe documentarse el motivo y su impacto en este `CLAUDE.md`, pero **no debe asumirse que la propuesta inicial está equivocada ni sustituirse su contenido por iniciativa propia**.

El **caso de uso definido en la propuesta es la detección de fraude en transacciones bancarias simuladas** y debe mantenerse como el caso de uso del proyecto salvo indicación explícita del usuario.

El hecho de que la plataforma de ingeniería de datos sea el protagonista del TFG no convierte el caso de uso en genérico ni permite cambiarlo automáticamente por otro dominio.

Si existe una discrepancia entre una decisión técnica posterior y la propuesta original, Claude Code debe:

1. Identificar la discrepancia.
2. Mantener la propuesta original intacta salvo indicación explícita.
3. Registrar la nueva decisión y el motivo del cambio en `CLAUDE.md`.
4. Utilizar la decisión más reciente para el desarrollo cuando haya sido confirmada por el usuario.

No modificar ni eliminar información de la propuesta original sin indicación explícita del usuario.

# 19. Reglas de mantenimiento de este archivo

Actualizar este archivo cuando se produzca un cambio significativo.

Debe actualizarse especialmente cuando:

- Se tome una decisión de arquitectura.
- Se descarte una tecnología.
- Se incorpore una tecnología.
- Se complete una fase.
- Se produzca un problema técnico relevante.
- Se obtenga un resultado experimental.
- Se modifique el alcance.
- Cambie la pregunta de investigación.
- Cambie el caso de uso.
- Se descubra una limitación importante.
- Se establezca un nuevo procedimiento operativo relevante.

Cada actualización debería incluir, cuando corresponda:

- Qué se hizo.
- Por qué se hizo.
- Qué alternativas se consideraron.
- Por qué se descartaron.
- Qué problemas surgieron.
- Cómo se resolvieron.
- Qué resultados se obtuvieron.
- Qué impacto tiene sobre el TFG.

No registrar únicamente afirmaciones genéricas como:

> "Se ha arreglado."

Registrar información útil para reconstruir posteriormente el proceso de desarrollo.

## Antes de `/compact`

Antes de compactar el contexto de una conversación:

1. Revisar las decisiones tomadas durante la sesión.
2. Registrar las decisiones relevantes en este archivo.
3. Registrar los problemas y soluciones.
4. Registrar las pruebas realizadas.
5. Registrar los resultados obtenidos.
6. Actualizar el estado de las fases.
7. Actualizar la guía de comandos si se ha descubierto algún procedimiento reutilizable.
8. Comprobar que no queda información importante únicamente en el contexto de la conversación.

## Estado de las fases

Una fase solo puede pasar de:

```text
[ ]
```

a:

```text
[x]
```

cuando esté **probada y funcionando de extremo a extremo**.

La existencia de código no implica que una fase esté terminada.

---

# 20. Regla fundamental del proyecto

Antes de añadir una nueva tecnología, funcionalidad o componente, Claude Code debe preguntarse:

> **¿Esto mejora realmente la plataforma o el experimento, o simplemente añade complejidad?**

El objetivo no es utilizar el mayor número posible de herramientas.

El objetivo es construir una **plataforma de ingeniería de datos coherente, reproducible, medible y defendible académicamente**, y utilizarla para responder mediante experimentación a la pregunta de investigación.

La arquitectura debe ser lo suficientemente completa para demostrar capacidades reales de Data Engineering, pero suficientemente controlada para poder implementarse, probarse y documentarse dentro del alcance del TFG.

## Regla adicional: no asumir decisiones no tomadas

Si una decisión importante todavía no está definida, Claude Code debe:

1. Detectar que existe una decisión pendiente.
2. Explicar brevemente las alternativas relevantes.
3. Recomendar una opción si tiene argumentos suficientes.
4. Esperar confirmación del usuario cuando la decisión pueda afectar significativamente a la arquitectura, el alcance o el cronograma.
5. No presentar una decisión provisional como definitiva.
6. **No cambiar por iniciativa propia el caso de uso de detección de fraude en transacciones bancarias simuladas.** Cualquier cambio de dominio debe ser solicitado explícitamente por el usuario.

## Regla adicional: aprender mientras se construye

El hecho de que una tecnología sea nueva para el usuario no debe ser motivo para evitarla automáticamente.

El proyecto debe avanzar de forma incremental:

1. Comprender el concepto.
2. Crear una versión mínima.
3. Ejecutarla.
4. Comprobar su funcionamiento.
5. Documentar el procedimiento.
6. Integrarla con el siguiente componente.
7. Probar nuevamente.

No intentar construir toda la arquitectura de una sola vez.

---

# 21. Estado real del repositorio (auditoría de lectura, 2026-09-11)

Esta sección recoge lo que **realmente existe en el repositorio** frente a lo que está planificado, para que futuras sesiones no tengan que volver a deducirlo leyendo todo el código. Es una foto del estado en la fecha indicada: debe verificarse contra el código antes de darla por buena y actualizarse cuando cambie.

## 21.1 Inventario de archivos relevantes

```text
docker-compose.yml                        # 8 servicios (ver 21.2)
sql/init.sql                              # tabla estado_cuenta (account shadow)
generator/generate_transactions.py        # generador de transacciones bancarias
generator/requirements.txt                # psycopg2-binary, Faker
debezium/register-connector.json          # conector Debezium "fraude-connector"
debezium/README.md                        # DESACTUALIZADO (habla de IoT)
README.md                                 # DESACTUALIZADO (habla de IoT)
spark-scripts/read_kafka_console.py       # Fase 3A: consumo CDC a consola
spark-scripts/read_kafka_to_bronze.py     # Fase 3B: escritura en bronze (Iceberg)
spark-scripts/detectar_anomalias.py       # Fase 3C: z-score -> silver
documentation/propuesta_tfg_fraude_bancario.md
notebooks/                                # vacío
warehouse/                                # vacío en local; ignorado por git
```

`.gitignore` contiene únicamente `warehouse/` y `venv/`. **Todavía no existe ningún `.env`**: las credenciales (`tfg` / `tfg_pass`, `admin` / `password`) están escritas directamente en `docker-compose.yml` y como valores por defecto en el generador. Esto contradice la convención de la sección 9 y debe resolverse antes de publicar el repositorio.

## 21.2 Servicios definidos en `docker-compose.yml`

| Servicio | Contenedor | Imagen | Puertos |
|---|---|---|---|
| postgres | `tfg-postgres` | `postgres:16` (`wal_level=logical`) | 5432 |
| zookeeper | `tfg-zookeeper` | `quay.io/debezium/zookeeper:2.7` | 2181 |
| kafka | `tfg-kafka` | `quay.io/debezium/kafka:2.7` | 9092 |
| connect | `tfg-connect` | `quay.io/debezium/connect:2.7` | 8083 |
| spark-iceberg | `tfg-spark` | `tabulario/spark-iceberg:3.5.5_1.8.1` | 8888, 8080, 10000, 10001 |
| rest | `tfg-iceberg-rest` | `apache/iceberg-rest-fixture` | 8181 |
| minio | `tfg-minio` | `minio/minio` | 9000, 9001 |
| mc | `tfg-mc` | `minio/mc` (crea el bucket `warehouse` y duerme) | — |

Notas operativas importantes:

- La base de datos se sigue llamando **`iot`** y el volumen **`pgdata`**, herencia del caso de uso anterior. El nombre no afecta al funcionamiento, pero conviene decidir si se renombra (implica recrear el volumen) o si se justifica en la memoria.
- El catálogo Iceberg se llama **`demo`** y viene preconfigurado en la imagen `tabulario/spark-iceberg` apuntando al catálogo REST (`tfg-iceberg-rest`) sobre MinIO. Por eso los scripts usan `demo.bronze.*` y `demo.silver.*` sin configurar nada más.
- Kafka es de **un solo broker y con ZooKeeper** (no KRaft). Los tópicos los crea Debezium automáticamente, por lo que por defecto tendrán **1 partición**. Para los experimentos sobre particionado de la sección 14.2 habrá que crear el tópico explícitamente con más particiones antes de arrancar el conector, o reconfigurar el conector.

## 21.3 Flujo implementado hoy

```text
generate_transactions.py --UPSERT--> Postgres.estado_cuenta
        --> Debezium (fraude-connector) --> Kafka topic fraude.public.estado_cuenta
                 |
                 +--> read_kafka_to_bronze.py  (trigger 10 s)  --> demo.bronze.eventos_cuenta
                 |
                 +--> detectar_anomalias.py    (trigger 15 s)  --> demo.silver.transacciones_validadas
```

Los dos jobs de Spark son **consumidores independientes del mismo tópico** y deben ejecutarse a la vez, cada uno con su propio `checkpointLocation` bajo `/home/iceberg/warehouse/_checkpoints/`.

Comando de ejecución de cada job (dentro del contenedor, en una terminal distinta cada uno):

```bash
docker exec -it tfg-spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 /home/iceberg/scripts/<script>.py
```

Según los mensajes de commit, este flujo se ha llegado a ejecutar y se observaron los micro-lotes por terminal, pero **no hay ninguna medición registrada**, por lo que ninguna fase puede marcarse como completada según la regla de la sección 19.

## 21.4 Parámetros ya implementados

| Parámetro | Dónde | Valor por defecto |
|---|---|---|
| `NUM_CUENTAS` | generador | 30 |
| `INTERVALO_SEGUNDOS` | generador | 2 |
| `PROB_ANOMALIA` | generador | 0.04 |
| `PROB_RECHAZO` | generador | 0.05 |
| `VENTANA_LECTURAS` | `detectar_anomalias.py` | 20 |
| `Z_SCORE_UMBRAL` | `detectar_anomalias.py` | 3.0 |

`VENTANA_LECTURAS` y `Z_SCORE_UMBRAL` están **hardcodeados** como constantes de módulo; para poder barrerlos en la matriz de experimentos habrá que parametrizarlos por variable de entorno o argumento.

## 21.5 Lo que falta respecto a la propuesta

Pendiente de implementar (no existe ni una versión mínima):

1. **Modo de pico de carga** en el generador (objetivo específico 1 y sección 2.4). Hoy la frecuencia es constante.
2. **Modo de eventos corruptos** en el generador. Hoy es imposible generar un evento malformado: el `INSERT` pasa por un esquema tipado de Postgres, así que **la corrupción no puede inyectarse en el origen tal y como está montado**. Habrá que decidir dónde se inyecta (por ejemplo, un productor Kafka paralelo que escriba directamente en el tópico, o campos de texto libre en la tabla). Es una decisión de diseño abierta y debe consultarse con el usuario.
3. **Dead-letter queue.** No existe. Los scripts actuales hacen `filter(col("id_cuenta").isNotNull())`, es decir, **descartan silenciosamente** lo que no encaja en el esquema en vez de aislarlo y contabilizarlo. Descartar no es lo mismo que aislar: la DLQ debe conservar el evento y su motivo de fallo.
4. **Capa gold**, **dbt Core**, **Airflow**, **DuckDB** y **dashboard**. Ninguno está en `docker-compose.yml`.
5. ~~Instrumentación de métricas.~~ **Resuelto el 2026-09-11.** Ver 21.9.
6. ~~Etiqueta de verdad (ground truth).~~ **Resuelto el 2026-09-11.** Ver 21.9.

## 21.6 Cuestiones técnicas detectadas que afectan al diseño

Estas observaciones son material directo para la memoria (secciones 7 y 17) y conviene contrastarlas experimentalmente antes de darlas por ciertas:

- **El detector lee su histórico de `bronze`, que lo escribe otro job.** El escritor dispara cada 10 s y el detector cada 15 s, así que la media y la desviación con las que se juzga una transacción dependen de una tabla que puede ir por detrás o por delante. En particular, la propia transacción que se está evaluando puede haber sido ya escrita en `bronze`, contaminando su propia media. Es un acoplamiento entre jobs que habrá que medir y justificar, o sustituir por estado en el propio stream (`flatMapGroupsWithState`) o por una ventana deslizante de Structured Streaming.
- **El cálculo del histórico hace un `ROW_NUMBER()` sobre toda la tabla `bronze` en cada micro-lote.** El coste crece con el histórico acumulado, no con el tamaño del lote. Es previsible que sea el primer cuello de botella al subir la carga, y por eso mismo es un buen objeto de estudio experimental.
- **`estado_cuenta` guarda solo la última transacción de cada cuenta.** Es intencionado (patrón account shadow, maximiza los `UPDATE` para el CDC), pero implica que Postgres no conserva el histórico y que el recuento de "eventos generados" para las métricas de integridad tiene que salir del generador, no de la base de datos.
- ~~Los README describen el caso de uso IoT anterior.~~ **Resuelto el 2026-09-11:** ambos reescritos y sus procedimientos verificados contra el entorno real. Ver la sección 7.

## 21.7 Historia del proyecto: cambio de dominio

El repositorio nació con un caso de uso de **sensores IoT** (tabla `estado_dispositivo`, patrón *device shadow*, ~30 sensores de temperatura y humedad) y se reconvirtió a **detección de fraude en transacciones bancarias** en el commit `af4c05a`. La conversión se hizo sobre el mismo esqueleto: el patrón *account shadow* es el antiguo *device shadow* y la lógica de detección por z-score es la misma que se aplicaba a las lecturas de los sensores.

Quedan restos de esa etapa anterior en el nombre de la base de datos (`iot`), en los dos README y en algún comentario del código. **No son errores que haya que ocultar**: documentan una decisión de diseño real y su motivo (el dominio cambió, la plataforma no tuvo que cambiar), lo cual respalda directamente la tesis de la sección 1.1 de que la plataforma es el protagonista y el dominio es intercambiable. Conviene contarlo así en la memoria en lugar de borrar el rastro.

## 21.8 Archivos ajenos al proyecto

`spark-scripts/Recordatorio.pdf` es un documento personal sin relación con el TFG que acabó en el repositorio por error (está sin seguimiento en git). No debe añadirse a ningún commit; lo razonable es moverlo fuera del repositorio. Antes de preparar el repositorio público (fase 11) conviene revisar que no queden más archivos de este tipo.

## 21.9 Cambios del 2026-09-11

Tres checkpoints, cada uno con su commit y verificado contra el entorno levantado.

**Documentación (`8e0ff19`).** `README.md` y `debezium/README.md` reescritos para el caso de uso de fraude bancario. El procedimiento de registro del conector se ejecutó de principio a fin: `201 Created` y estado `RUNNING`.

**Etiqueta de verdad (`8c04139`).** `estado_cuenta` incorpora dos columnas nuevas:

| Columna | Tipo | Valores |
|---|---|---|
| `es_anomalia_generada` | `BOOLEAN NOT NULL DEFAULT false` | si el generador la inyectó como anómala |
| `tipo_anomalia_generada` | `VARCHAR(30)` | `NULL`, `importe`, `importe_ubicacion` |

La etiqueta atraviesa todo el pipeline hasta `silver`, donde queda junto a la predicción del detector (`es_anomalia`), de modo que la matriz de confusión sale de una sola consulta. **Regla de diseño que no se puede romper:** ninguna lógica de detección lee esas columnas. El z-score se calcula solo con el importe y el histórico de la cuenta. Si el detector mirase la etiqueta, el experimento dejaría de medir nada.

Los dos jobs que escriben en Iceberg incluyen una función `asegurar_columnas` que añade a una tabla ya existente las columnas que le falten, porque `CREATE TABLE IF NOT EXISTS` no evoluciona el esquema de una tabla anterior. Se apoya en la evolución de esquema de Iceberg, que no reescribe los datos: las filas antiguas devuelven `NULL` en la columna nueva. Es, además, una demostración práctica de una de las capacidades por las que se eligió Iceberg, y como tal sirve para la memoria.

**Instrumentación de métricas (`ebab1af`).** Dos piezas nuevas:

- `spark-scripts/metricas.py`: un `StreamingQueryListener` registrado en los dos jobs de streaming. Escribe una fila por micro-lote en `warehouse/_metricas/<job>_<marca>.csv` con filas de entrada, throughput, duración del micro-lote, duración de la escritura y consumer lag, y vuelca el progreso íntegro de Spark en un `.jsonl` paralelo por si más adelante hace falta una métrica que hoy no está entre las columnas elegidas.
- `scripts/recolectar_recursos.py`: muestrea `docker stats` desde el anfitrión y registra CPU y memoria por contenedor, que es lo que el listener no puede ver desde dentro.

**Detalle técnico importante para la evaluación:** el consumer lag se toma de `maxOffsetsBehindLatest`, que publica la propia fuente de Kafka en el evento de progreso. Es la vía correcta porque **Spark Structured Streaming no confirma offsets contra Kafka**: usa su propio checkpoint. Por eso `kafka-consumer-groups.sh --describe` no ve estos consumidores y no sirve para medir su lag, que es el camino que uno esperaría a primera vista.

### Parámetros y rutas que esto añade

| Elemento | Valor por defecto | Dónde |
|---|---|---|
| `RUTA_METRICAS` | `/home/iceberg/warehouse/_metricas` | variable de entorno leída por `metricas.py` |
| `--intervalo` | 5 s | `recolectar_recursos.py` |
| `--duracion` | 0 (hasta `Ctrl+C`) | `recolectar_recursos.py` |
| `--etiqueta` | `recursos` | prefijo del CSV, útil para nombrar cada experimento |

Las métricas caen en `warehouse/_metricas/`, dentro de un directorio ya excluido del control de versiones, así que las medidas de un mismo experimento quedan juntas y no se versionan por accidente.

### Decisiones de esta sesión

1. ~~Añadir `__pycache__/` a `.gitignore`.~~ **Aprobado y aplicado** (`5047f9b`).
2. ~~Eliminar el replication slot huérfano `iot_slot`.~~ **Aprobado y aplicado.** Ver la sección 7.
3. ~~Dar persistencia al lakehouse.~~ **Aprobada la opción A y aplicada.** Volúmenes con nombre para MinIO y para el catálogo Iceberg. Verificado con un `down` + `up` completo: las 2.511 filas de `bronze` siguen ahí. El análisis de alternativas y el motivo de la elección están en las secciones 6 y 7. Al verificarlo se descubrió que Kafka y el conector tienen el mismo problema, que queda documentado y sin resolver a propósito (sección 7).
4. **Rama única `main`.** Decidido por el usuario. Los siete commits de la sesión se llevaron de `tfg` a `main` mediante un avance rápido. Ver las secciones 6 y 10.

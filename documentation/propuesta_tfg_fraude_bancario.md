# PROPUESTA DE TRABAJO DE FIN DE GRADO

**Arquitectura de ingesta de datos en streaming mediante Change Data Capture: detección de anomalías y resiliencia operacional ante perturbaciones de carga y corrupción de eventos**

**Caso de uso:** detección de fraude en transacciones bancarias simuladas

**Autor/a:** [Tu nombre]  
**Tutor/a:** [Nombre del tutor o tutora]  
**Titulación:** Grado en Ingeniería Informática / Ciencia e Ingeniería de Datos  
**Curso académico:** 2026/2027

## Índice

1. Introducción y motivación
2. Objetivos
3. Alcance del proyecto
4. Caso de uso: detección de fraude en transacciones bancarias simuladas
5. Arquitectura y metodología técnica
6. Stack tecnológico
7. Metodología de trabajo
8. Plan de trabajo y cronograma
9. Resultados esperados
10. Riesgos y mitigación
11. Referencias preliminares

## 1. Introducción y motivación

En los últimos años, buena parte de la ingeniería de datos ha ido desplazando su centro de gravedad desde los procesos ETL por lotes, ejecutados de forma periódica, hacia arquitecturas orientadas a eventos que procesan la información a medida que se genera. Este cambio responde a una necesidad práctica: muchas decisiones de negocio pierden valor si se toman con datos de horas o días de antigüedad.

Paralelamente, el almacenamiento analítico ha evolucionado hacia el paradigma “data lakehouse”, que combina la flexibilidad y el coste de un data lake con las garantías transaccionales y de gobernanza propias de un data warehouse, gracias a formatos de tabla abiertos como Apache Iceberg o Delta Lake.

Sin embargo, la mayoría de los pipelines de streaming no fallan en su “camino feliz” inicial, sino ante lo imprevisto: una ráfaga de tráfico muy superior a la habitual, o un evento que llega corrupto o mal formado. Por ello, este TFG no se limita a construir la infraestructura de ingesta: propone diseñar e implementar, con tecnología íntegramente gratuita y de código abierto, un pipeline de datos de extremo a extremo que capture cambios en tiempo real mediante Change Data Capture (CDC), detecte anomalías en el flujo de transacciones a medida que llegan y sea capaz de adaptarse a picos de carga y a eventos corruptos sin detener el sistema, aplicado a la detección de fraude en transacciones bancarias simuladas.

## 2. Objetivos

### 2.1 Objetivo general

Diseñar, implementar y evaluar un pipeline de datos de extremo a extremo que capture cambios en tiempo real mediante CDC, detecte anomalías en el flujo de transacciones y sea capaz de adaptarse a imprevistos operativos —picos de carga y eventos corruptos— sin perder disponibilidad, integrándolo en una arquitectura data lakehouse con un enfoque de transformación ELT moderno, utilizando exclusivamente herramientas gratuitas y de código abierto.

### 2.2 Objetivos específicos

1. Diseñar un generador de datos sintéticos que simule transacciones bancarias sobre un conjunto de cuentas, con inyección controlada de importes anómalos, picos de carga y eventos corruptos.
2. Implementar la captura de cambios (CDC) sobre la tabla de estado de las cuentas mediante Debezium y un sistema de mensajería tipo Kafka.
3. Desarrollar un proceso de streaming que persista los eventos capturados en una capa “bronze” con un formato de tabla abierto (Apache Iceberg).
4. Implementar un mecanismo de detección de anomalías en tiempo real sobre el flujo de transacciones, basado en estadísticas por ventana deslizante (media y desviación) por cuenta.
5. Diseñar el pipeline para absorber picos de carga repentinos sin pérdida de eventos, evaluando el comportamiento de Kafka y Spark Structured Streaming bajo distintos niveles de estrés.
6. Implementar una cola de eventos fallidos (dead-letter queue) que aísle los eventos corruptos o malformados sin detener el procesamiento del resto del flujo.
7. Definir e implementar transformaciones ELT hacia las capas “silver” y “gold” mediante dbt, orquestadas de forma periódica.
8. Habilitar una capa de consulta y un panel de visualización con las alertas de anomalías y el estado de la cola de eventos fallidos.
9. Evaluar el sistema mediante experimentos controlados de picos de carga y eventos corruptos, midiendo su impacto en la latencia, el throughput y la integridad de los datos.

## 3. Alcance del proyecto

Con el fin de mantener el proyecto dentro de un tamaño razonable para un TFG de un cuatrimestre, se establecen de forma explícita los límites del trabajo:

### Incluye

- Una única fuente de datos (tabla de estado de cuentas bancarias) con anomalías, picos de carga y eventos corruptos inyectados de forma controlada.
- Pipeline CDC funcional de extremo a extremo hasta la capa gold.
- Detección de anomalías en tiempo real mediante estadísticas por ventana deslizante.
- Un mecanismo de absorción de picos de carga y una cola de eventos fallidos (dead-letter queue).
- Orquestación automatizada de las transformaciones y un panel con alertas.
- Evaluación cuantitativa de latencia, throughput y comportamiento ante los imprevistos inyectados.

### No incluye

- Múltiples fuentes de datos o sistemas de pago distintos (TPV físicos, pasarelas de pago externas, etc.).
- Modelos de machine learning complejos para detectar anomalías (se usan métodos estadísticos simples).
- Alta disponibilidad y tolerancia a fallos más allá de los escenarios de prueba definidos.
- Seguridad avanzada, autenticación o multi-tenencia.
- Despliegue en infraestructura cloud de pago.
- Datos reales de clientes o de una entidad bancaria real.

## 4. Caso de uso: detección de fraude en transacciones bancarias simuladas

Se ha elegido deliberadamente un caso de uso sencillo, con un único dominio y una única tabla origen, para que la complejidad del proyecto resida en la arquitectura de datos y no en el modelado del negocio.

El escenario simula un conjunto de unas 30 cuentas bancarias que van generando transacciones de distintos tipos (compra online, compra presencial, retirada en cajero, transferencia) de forma periódica. En lugar de modelar cada transacción como un registro histórico independiente, se adopta el patrón “account shadow” (una variante del “device shadow” habitual en sistemas de streaming): cada cuenta tiene una única fila en la tabla que representa su transacción más reciente, y esa fila se actualiza cada vez que se produce una nueva operación.

La tabla estado_cuenta, alojada en PostgreSQL, contiene los siguientes campos: id_cuenta, tipo_transaccion (compra_online, compra_presencial, retirada_cajero, transferencia), categoria_comercio (solo aplica a las compras; es NULL en retiradas y transferencias), canal (online, presencial, cajero, app_movil), ubicacion, importe, moneda, resultado (aprobada o rechazada), estado (activa o bloqueada) y fecha_actualizacion. De forma deliberada, si una transacción es anómala no se decide en el origen: esa decisión se traslada a la capa de streaming (ver sección 5), para que el experimento de detección sea representativo de un escenario real, en el que el productor de datos no sabe de antemano qué es fraudulento.

Un script en Python, apoyado en la librería Faker, simula el conjunto de cuentas y actualiza periódicamente (cada pocos segundos) la fila correspondiente a cada una con una nueva transacción, generando así muchas más actualizaciones que inserciones, lo cual es ideal para el CDC. Para poder evaluar la detección de fraude de forma objetiva, el generador inyecta de forma controlada un pequeño porcentaje de transacciones con importes anómalos (por ejemplo, un 3-5% de los eventos) y, en la mitad de esos casos, también desde una ubicación distinta a la habitual de esa cuenta —sentando la base para, en una fase posterior, detectar patrones de “viaje imposible”—, lo que permite calcular métricas de precisión y exhaustividad sobre lo que el pipeline detecta frente a lo que realmente se generó como anómalo.

El generador incorporará, además de la inyección de anomalías estadísticas, dos modos adicionales de estrés controlado: un modo de pico de carga, en el que un subconjunto de cuentas emite ráfagas de transacciones muy por encima de su frecuencia habitual (simulando, por ejemplo, un ataque de “card testing”, en el que se prueban muchos cargos pequeños en poco tiempo), y un modo de eventos corruptos, en el que se generan deliberadamente transacciones malformadas (tipos incorrectos, campos ausentes, valores no numéricos) en un pequeño porcentaje de los eventos.

Durante la fase de evaluación, ambos modos de estrés se activarán de forma controlada sobre el pipeline en funcionamiento, para medir cómo se comportan Kafka y Spark Structured Streaming ante un pico de carga (latencia, lag del consumidor, eventos perdidos) y para comprobar que la cola de eventos fallidos aísla correctamente los eventos corruptos sin detener el resto del flujo.

## 5. Arquitectura y metodología técnica

El sistema sigue una arquitectura en capas (bronze, silver, gold), habitual en los data lakehouse modernos, alimentada por un flujo continuo de eventos capturados mediante CDC:

| Etapa | Componente | Función |
|---|---|---|
| Fuente | PostgreSQL | Base de datos que registra la transacción más reciente de cada cuenta bancaria |
| Captura de cambios | Debezium | Detecta inserciones y actualizaciones en la tabla de estado de cuentas y las publica como eventos |
| Mensajería | Apache Kafka (o Redpanda) | Transporta los eventos de cambio de forma duradera y desacoplada, absorbiendo picos de carga mediante particionado y buffering |
| Procesamiento en streaming | Spark Structured Streaming | Consume los eventos, valida su estructura y los prepara para su análisis y persistencia |
| Cola de eventos fallidos | Tópico Kafka dedicado (dead-letter queue) | Aísla los eventos corruptos o malformados sin detener el procesamiento del resto del flujo |
| Detección de anomalías | Spark Structured Streaming (ventana deslizante) | Calcula media y desviación del importe por cuenta y marca cada transacción como normal o sospechosa en tiempo real |
| Capa bronze | Apache Iceberg sobre MinIO | Almacena los datos crudos y versionados |
| Transformación ELT | dbt Core + Spark SQL | Limpia, valida y agrega los datos en las capas silver y gold |
| Orquestación | Apache Airflow | Programa y supervisa la ejecución periódica de las transformaciones |
| Consulta y visualización | DuckDB + Metabase/Streamlit | Expone los datos, las alertas de fraude y el estado de la cola de eventos fallidos |

El diseño contempla explícitamente dos mecanismos de adaptabilidad ante imprevistos: por un lado, Kafka actúa como amortiguador entre la captura de cambios y el procesamiento, absorbiendo picos de carga sin bloquear la fuente ni al conector CDC; por otro, todo evento que no supere una validación básica de estructura se enruta a un tópico de eventos fallidos en vez de detener el job de streaming, permitiendo su inspección y reprocesado posterior. Ambos mecanismos se pondrán a prueba de forma deliberada durante la fase de evaluación (ver sección 9).

Todo el entorno se despliega mediante Docker Compose en una única máquina de desarrollo, sin dependencias de servicios cloud de pago.

## 6. Stack tecnológico

Todas las tecnologías seleccionadas son gratuitas y de código abierto. La siguiente tabla detalla la herramienta elegida en cada capa junto con su modelo de licencia:

| Capa | Tecnología | Licencia |
|---|---|---|
| Base de datos origen | PostgreSQL | PostgreSQL License (código abierto) |
| Generación de datos sintéticos | Python + Faker | MIT (código abierto) |
| Captura de cambios (CDC) | Debezium | Apache 2.0 (código abierto) |
| Mensajería / streaming | Apache Kafka | Apache 2.0 (código abierto) |
| Procesamiento en streaming | Apache Spark | Apache 2.0 (código abierto) |
| Formato de tabla / lakehouse | Apache Iceberg | Apache 2.0 (código abierto) |
| Almacenamiento de objetos | MinIO (edición Community) | AGPLv3 (código abierto) |
| Transformaciones ELT | dbt Core | Apache 2.0 (código abierto, no confundir con dbt Cloud) |
| Orquestación | Apache Airflow | Apache 2.0 (código abierto) |
| Consulta analítica | DuckDB | MIT (código abierto) |
| Visualización | Metabase Open Source / Streamlit | AGPLv3 / Apache 2.0 (código abierto) |
| Contenerización | Docker y Docker Compose | Gratuito para uso educativo y personal |

**Nota:** se recomienda usar Docker Engine (Linux) o Docker Desktop en modo personal/educativo, y evitar distribuciones o versiones “cloud” o “enterprise” de estas herramientas, que suelen requerir licencia de pago a partir de cierto volumen de uso.

## 7. Metodología de trabajo

El proyecto se desarrollará de forma incremental: primero se construirá una versión mínima del pipeline funcionando de extremo a extremo, aunque sea con componentes simplificados, y a partir de ahí se irán añadiendo capacidades (procesamiento en streaming, transformaciones ELT, orquestación, evaluación). Este enfoque reduce el riesgo de quedarse sin un sistema demostrable si el tiempo se ajusta.

Se realizarán reuniones periódicas de seguimiento con el tutor o tutora al final de cada fase del cronograma (ver sección 8), en las que se validará el alcance y se ajustará la planificación si fuera necesario.

## 8. Plan de trabajo y cronograma

Se plantea una duración total de 16 semanas, compatible con un cuatrimestre académico:

| Semanas | Fase | Actividades principales | Entregable |
|---|---|---|---|
| 1-2 | Estado del arte y diseño | Revisión de CDC, arquitecturas lakehouse, detección de fraude y mecanismos de resiliencia en streaming; definición final de alcance | Documento de diseño técnico |
| 3-4 | Entorno base | Configuración de Docker Compose, PostgreSQL, Kafka/Redpanda y MinIO | Entorno de desarrollo funcional |
| 5-6 | Generador de datos y CDC | Script de generación de transacciones bancarias sintéticas con inyección controlada de importes anómalos, picos de carga y eventos corruptos; configuración de Debezium | Transacciones etiquetadas, capturadas como eventos CDC |
| 7-9 | Streaming, anomalías y capa bronze | Job de Spark Structured Streaming; cálculo de estadísticas por ventana deslizante; escritura en Iceberg | Capa bronze con transacciones marcadas como normales o sospechosas |
| 10-11 | Mecanismos de resiliencia | Implementación de la cola de eventos fallidos y ajuste de la absorción de picos de carga en Kafka/Spark | Pipeline con dead-letter queue y control de backpressure funcionando |
| 12 | Transformaciones ELT | Modelos dbt para las capas silver y gold; orquestación con Airflow | Capas silver/gold con métricas por cuenta y ubicación |
| 13-14 | Consulta y visualización | Configuración de DuckDB y panel en Metabase/Streamlit con alertas de fraude y estado de la DLQ | Panel de analítica funcional |
| 15 | Evaluación | Medición de latencia y precisión de la detección de fraude; pruebas controladas de picos de carga y eventos corruptos | Informe de resultados |
| 16 | Cierre | Redacción final de la memoria y preparación de la defensa | Memoria del TFG completa |

## 9. Resultados esperados

1. Un pipeline funcional que demuestre la ingesta en tiempo real mediante CDC hacia una arquitectura lakehouse, con una capa de detección de fraude integrada.
2. Un mecanismo de absorción de picos de carga y una cola de eventos fallidos, evaluados mediante pruebas de estrés controladas.
3. Métricas de precisión y exhaustividad de la detección de fraude, calculadas frente a las anomalías inyectadas de forma controlada en el generador sintético.
4. Métricas de latencia y throughput del pipeline bajo picos de carga, y una medición de cuántos eventos corruptos quedan correctamente aislados en la cola de eventos fallidos.
5. Una arquitectura de referencia documentada y reproducible, útil como plantilla para proyectos similares de tamaño reducido.
6. Un repositorio de código público con el docker-compose y las instrucciones necesarias para reproducir el entorno.
7. La memoria del TFG y el material de apoyo para su defensa.

## 10. Riesgos y mitigación

| Riesgo | Mitigación |
|---|---|
| Complejidad de integrar varias tecnologías nuevas de forma simultánea | Construir primero una versión mínima de extremo a extremo y añadir componentes de forma incremental |
| Recursos limitados en la máquina de desarrollo (Spark, Kafka y MinIO consumen bastante memoria) | Usar alternativas más ligeras si es necesario: Redpanda en vez de Kafka, DuckDB en vez de Trino |
| Curva de aprendizaje de herramientas nuevas (Debezium, Iceberg, dbt) | Reservar las dos primeras semanas del cronograma a documentación oficial y tutoriales guiados |
| Desviación del alcance definido (“scope creep”) | Revisar el alcance de la sección 3 en cada hito de seguimiento con el tutor o tutora |
| Definir umbrales de anomalía poco realistas o difíciles de justificar | Empezar con un método simple y bien documentado (z-score) y justificar los parámetros con los propios datos sintéticos generados |
| Los picos de carga simulados no sean lo bastante intensos para generar degradación observable | Calibrar la intensidad del pico en función de la capacidad real de la máquina de desarrollo, aumentándola progresivamente hasta observar el efecto |
| La cola de eventos fallidos se llene sin control o se convierta en ruido inmanejable | Limitar el porcentaje de eventos corruptos inyectados (2-3%) y definir un proceso claro de revisión y reprocesado de la DLQ |

## 11. Referencias preliminares

1. Documentación oficial de Debezium: debezium.io/documentation
2. Documentación oficial de Apache Iceberg: iceberg.apache.org
3. Documentación oficial de dbt: docs.getdbt.com
4. Documentación oficial de Apache Airflow: airflow.apache.org
5. Documentación oficial de Apache Kafka (particionado, buffering y control de flujo): kafka.apache.org/documentation
6. Kleppmann, M. *Designing Data-Intensive Applications*. O'Reilly Media.
7. Chandola, V., Banerjee, A., y Kumar, V. *Anomaly Detection: A Survey*. ACM Computing Surveys.
8. Dal Pozzolo, A., Caelen, O., Le Borgne, Y.-A., Waterschoot, S., y Bontempi, G. *Credit Card Fraud Detection: A Realistic Modeling and a Novel Learning Strategy*. IEEE Transactions on Neural Networks and Learning Systems.
# TAREAS — Estado del proyecto y plan de trabajo

Documento de seguimiento del TFG. Recoge el estado real del proyecto y las 90
tareas del plan por fases, explicada cada una, indicando cuáles están terminadas
y qué implica cada una de las que quedan.

**Última actualización:** 2026-09-11 · **30 de 90 tareas completadas (33 %)**

Una tarea solo se marca como hecha cuando se ha **probado y funciona de extremo a
extremo**. Que el código exista no basta.

---

## 1. Resumen del estado

Lo que yo haría, por orden a 11/09/26

  1. Eventos rotos + DLQ. Cierra la fase 4 y es tu segunda aportación diferencial.
  2. Ajustar el detector con el 4 % de anomalías, para que las métricas signifiquen algo.
  3. gold + dbt. Es mucho más fácil de lo que parece: son ficheros SQL.
  4. Los experimentos. Se pueden empezar ya, sin esperar al panel.
  5. DuckDB y el panel. Lo último, porque es lo más prescindible si vas justo de tiempo

El proyecto tiene un pipeline de datos funcionando de principio a fin, que ya
produce métricas cuantitativas. El flujo actual es:

```text
generador Python ──► PostgreSQL ──CDC──► Debezium ──► Kafka
                                                        │
                                    ┌───────────────────┴──────────────┐
                                    ▼                                  ▼
                              capa bronze                      detección z-score
                            (Iceberg/MinIO)                      capa silver
```

**Fases completas y verificadas:**

- **Fase 3 — Entorno base.** Ocho servicios en Docker Compose comunicándose entre sí. El lakehouse persiste entre reinicios.
- **Fase 6 — Detección de anomalías.** Z-score por ventana deslizante sobre las últimas 20 transacciones de cada cuenta, con etiqueta de verdad que permite medir precisión y exhaustividad.

**Fases parciales:**

- **Fase 1 — Definición.** Todas las decisiones de alcance tomadas y registradas. Faltan el estado del arte, la validación con el tutor y la arquitectura definitiva.
- **Fase 4 — Generador y CDC.** Generación, captura y modo de pico de carga funcionando. Falta el modo de eventos corruptos.
- **Fase 5 — Streaming y bronze.** Ingesta y persistencia funcionando. Falta separar los eventos corruptos.

**Sin empezar:** fases 2, 8, 9, 10 y 11.

**Fuera del plan original pero ya construido:** instrumentación completa de
métricas. Throughput, latencia, consumer lag, CPU y RAM se registran
automáticamente en CSV. Era el requisito previo para poder responder a la
pregunta de investigación.

### Métricas ya medidas

Dos mediciones del 2026-09-11. Son líneas base de verificación, no experimentos
formales.

#### Régimen normal

| Métrica | Valor |
|---|---|
| Eventos procesados | 2.511 |
| Throughput | 114–339 filas/s |
| Consumer lag | 0 |
| Latencia media de extremo a extremo | 10,89 s |
| Latencia máxima | 87 s (arranque en frío) |
| Precisión de la detección | 100 % |
| Exhaustividad (recall) | 9,3 % |
| Pico de CPU de Spark | 744 % |
| Memoria de Spark | 4,2 GiB de 7,9 disponibles |

La exhaustividad del 9,3 % **no es un fallo del detector**: la prueba se lanzó
con un 30 % de transacciones anómalas, y con esa proporción son las propias
anomalías las que inflan la media y la desviación sobre las que se calcula el
z-score. Hay que repetirla con el valor por defecto del 4 % antes de juzgar la
calidad de la detección.

#### Primer pico de carga

Ráfaga de 30 s sobre 5 cuentas, dentro de una ejecución de 150 s.

| Régimen | Generación | Filas por micro-lote | Throughput de Spark | Duración del micro-lote |
|---|---|---|---|---|
| Normal | 1 evento/s | ~10 | ~10 filas/s | ~1.000 ms |
| Pico | **692 eventos/s** | hasta **6.974** | hasta **7.369 filas/s** | 927–1.074 ms |

20.890 eventos generados, 20.770 de ellos dentro de la ráfaga. Las 22.828
transacciones de card testing llegaron a `bronze` con un importe medio de 1,75 €.

### Tres limitaciones que condicionan lo que queda

- **Spark consume 4,2 de los 7,9 GiB disponibles para Docker.** Es el techo real de la máquina. Conviene comprobar que Airflow y Metabase caben antes de añadirlos.
- **El consumer lag solo informa si se acota el micro-lote.** Sin `maxOffsetsPerTrigger`, Spark consume todo lo disponible en cada disparo y la métrica da cero por construcción. Ya está resuelto: los jobs aceptan `MAX_OFFSETS_POR_TRIGGER` e `INTERVALO_TRIGGER_SEGUNDOS`, y con ellos el lag describe una rampa limpia. **Hay que acordarse de fijarlos en cada experimento**, porque por defecto no hay límite.
- **El generador es hoy el cuello de botella, no la plataforma.** Su techo está en unos 692 eventos/s, y Spark los procesó en micro-lotes de ~1 s frente a un *trigger* de 10 s. Para provocar degradación observable ya no hace falta acelerarlo: basta con estrangular el consumo acotando el micro-lote.

---

## 2. Las 90 tareas

**Leyenda:** `[x]` terminada y verificada · `[ ]` pendiente

### Fase 1 — Definición y estado del arte

- [x] **1. Definir enfoque principal en Ingeniería de Datos.** Decidido: el TFG es de ingeniería de datos, no de ciencia de datos. La ciencia de datos aparece solo como componente secundario en la detección de anomalías. Registrado en la sección 6 de `CLAUDE.md`.
- [x] **2. Decidir que la plataforma será el protagonista del TFG.** El objeto de estudio es la plataforma; el caso de uso bancario es el escenario que permite medirla. Esta decisión es la que justifica que el dominio pudiera cambiar de IoT a banca sin rehacer la arquitectura.
- [x] **3. Definir una pregunta de investigación provisional.** «¿Cómo afectan la carga de trabajo y distintos parámetros de configuración al rendimiento de una plataforma de ingeniería de datos desplegada íntegramente con Docker?» Es provisional y puede refinarse si durante el diseño aparecen mejores variables experimentales.
- [x] **4. Establecer como requisito que todo el proyecto sea gratuito.** Todas las herramientas del stack son libres o de código abierto, con su licencia documentada en la propuesta. No se introducen servicios de pago.
- [x] **5. Establecer Docker como entorno de despliegue.** Toda la plataforma se levanta con Docker Compose en una sola máquina. Es lo que hace el proyecto reproducible y lo que enmarca la pregunta de investigación.
- [x] **6. Confirmar el caso de uso como definitivo.** Detección de fraude en transacciones bancarias simuladas. Fijado: no se cambia de dominio salvo decisión explícita.
- [ ] **7. Completar el estado del arte.** Revisión bibliográfica de CDC, arquitecturas lakehouse, formatos de tabla abiertos, detección de anomalías y mecanismos de resiliencia en streaming. La propuesta ya incluye ocho referencias preliminares que sirven de punto de partida. **Va en el camino crítico de la memoria y no depende del código**, así que puede avanzarse en paralelo al desarrollo.
- [ ] **8. Validar definitivamente el alcance con el tutor.** Reunión de validación del alcance de la sección 5 y de la pregunta de investigación. Es un bloqueo externo: conviene no dejarlo para el final, porque un cambio de alcance obliga a rehacer planificación.
- [ ] **9. Diseñar la arquitectura técnica definitiva.** Cerrar qué componentes entran de verdad y cuáles se descartan, a la vista de lo que la máquina aguanta. Ahora mismo hay tecnologías planificadas (Airflow, dbt, Metabase) cuya viabilidad conjunta no está comprobada.

### Fase 2 — Diseño técnico

> **Entregable de la fase:** documento de diseño técnico.

Varias de estas decisiones ya están tomadas de hecho en el código, pero **no
existe el documento** que las recoja y justifique de forma unificada, que es lo
que la fase entrega. Por eso ninguna está marcada.

- [ ] **10. Definir arquitectura definitiva.** Formalizar el diagrama de componentes y las responsabilidades de cada capa, incluida la decisión de qué se descarta.
- [ ] **11. Definir flujo completo de datos.** Documentar el recorrido de un evento desde que se genera hasta que llega a `gold`, con los puntos donde se transforma, se valida y se persiste.
- [ ] **12. Definir modelo de datos.** La tabla `estado_cuenta` ya existe con el patrón *account shadow* y sus columnas de etiqueta de verdad. Falta documentar formalmente el modelo y justificar por qué una fila por cuenta en vez de un histórico de transacciones.
- [ ] **13. Definir esquema de eventos.** El sobre CDC de Debezium (`payload.before`, `payload.after`, `op`, `source`) y qué campos consume cada job. Implementado, sin documentar.
- [ ] **14. Definir estrategia CDC.** Justificar `pgoutput` como plugin de decodificación lógica, `decimal.handling.mode: double`, la gestión de los *replication slots* y el modo de creación de la publicación.
- [ ] **15. Definir estrategia de particionado.** Doble decisión: particiones del tópico de Kafka (hoy **1**, creada automáticamente por Debezium) y particionado de las tablas Iceberg (hoy **ninguno**). Ambas afectan directamente al rendimiento, así que son candidatas naturales a variable experimental.
- [ ] **16. Definir estrategia de almacenamiento.** Organización de `bronze`, `silver` y `gold`, política de retención, compactación de ficheros pequeños en Iceberg y gestión de instantáneas.
- [ ] **17. Definir validaciones.** Qué hace que un evento sea válido. Es el requisito previo de la DLQ: sin un criterio explícito de validez no se puede decidir qué se aísla.
- [ ] **18. Definir funcionamiento de la DLQ.** Qué se guarda de cada evento fallido (contenido original, motivo, marca temporal, offset), en qué tópico, y cómo se inspecciona y reprocesa.
- [ ] **19. Definir métricas de evaluación.** Las métricas ya se están capturando; falta fijar formalmente cuáles entran en la memoria, cómo se calculan y con qué unidades.
- [ ] **20. Definir experimentos.** La matriz de experimentación: qué variables se barren, con qué valores, cuántas repeticiones y de qué duración.

### Fase 3 — Entorno base ✅ **COMPLETA**

> **Entregable:** entorno de desarrollo funcional. **Conseguido.**

- [x] **21. Configurar Docker Compose.** Ocho servicios: `postgres`, `zookeeper`, `kafka`, `connect`, `spark-iceberg`, `rest` (catálogo Iceberg), `minio` y `mc`. Con volúmenes con nombre para los datos que deben persistir.
- [x] **22. Configurar PostgreSQL.** PostgreSQL 16 con `wal_level=logical`, que es el requisito para el CDC, y la tabla `estado_cuenta` creada automáticamente desde `sql/init.sql`.
- [x] **23. Configurar Kafka.** Kafka de un solo broker con ZooKeeper, imágenes de Debezium 2.7. Se descartó Redpanda porque Kafka funciona sin problemas de recursos en esta máquina.
- [x] **24. Configurar MinIO.** Almacenamiento de objetos compatible con S3, con el bucket `warehouse` creado automáticamente y volumen con nombre para que sobreviva a `docker compose down`.
- [x] **25. Comprobar comunicación entre servicios.** Verificado de extremo a extremo el 2026-09-11: el generador escribe en Postgres, Debezium captura, Kafka transporta y Spark lee y escribe en Iceberg sobre MinIO.
- [x] **26. Documentar cómo levantar y detener el entorno.** En `README.md` y en la sección 13 de `CLAUDE.md`, con procedimientos ejecutados y verificados, no hipotéticos.

### Fase 4 — Generador de datos y CDC

> **Entregable:** datos sintéticos capturados como eventos CDC. **Conseguido, con el modo de pico de carga incluido; falta el de eventos corruptos.**

- [x] **27. Implementar generador de transacciones bancarias.** `generator/generate_transactions.py`, en Python con Faker. Simula cuentas que actualizan su fila en `estado_cuenta`, generando muchos más `UPDATE` que `INSERT`, que es el escenario donde el CDC aporta valor.
- [x] **28. Implementar generación de eventos normales.** Cuatro tipos de transacción (compra online, compra presencial, retirada en cajero y transferencia), cada uno con su canal, su rango de importe normal y su categoría de comercio cuando aplica.
- [x] **29. Implementar inyección controlada de anomalías.** Parámetro `PROB_ANOMALIA`. Genera importes fuera del rango del tipo y, la mitad de las veces, también desde una ciudad distinta a la habitual de la cuenta. Persiste la etiqueta de verdad en `es_anomalia_generada` y `tipo_anomalia_generada`.
- [x] **30. Implementar modo de pico de carga.** `PICO_ACTIVO=true` alterna el ritmo normal con ráfagas de *card testing*: unas pocas cuentas emitiendo muchos cargos pequeños. Configurable en intensidad, duración, número de ráfagas, cuentas implicadas y momento de inicio. Las ventanas se planifican de antemano y se registran con marcas temporales absolutas en el resumen JSON, para poder superponerlas sobre las gráficas de métricas. Verificado: salto de 1 a **692 eventos/s**, micro-lotes de Spark de 10 a 6.974 filas, y 22.828 transacciones de la ráfaga persistidas en `bronze`.
- [ ] **31. Implementar modo de eventos corruptos.** Eventos malformados: tipos incorrectos, campos ausentes, valores no numéricos. **Tiene una decisión de diseño abierta:** no se puede inyectar corrupción a través de PostgreSQL, porque su esquema es tipado y rechaza los datos inválidos. Hay que elegir entre un productor Kafka paralelo que escriba directamente en el tópico, o campos de texto libre en la tabla.
- [x] **32. Configurar Debezium.** Conector `fraude-connector` sobre `public.estado_cuenta`, publicando en `fraude.public.estado_cuenta`. Configuración documentada clave por clave en `debezium/README.md`.
- [x] **33. Comprobar captura de INSERT/UPDATE.** Verificado: cada `UPSERT` del generador aparece como evento CDC con su `op` correspondiente.
- [x] **34. Comprobar publicación correcta en Kafka.** Verificado inspeccionando el `payload.after` de un evento real, con todos los campos —incluida la etiqueta de verdad— y los importes como números, no como bytes en Base64.

### Fase 5 — Streaming y capa Bronze

> **Entregable:** pipeline streaming funcional hasta `bronze`. **Conseguido; falta la separación de corruptos.**

- [x] **35. Implementar Spark Structured Streaming.** Tres jobs en `spark-scripts/`: uno de inspección por consola, uno de escritura en `bronze` y uno de detección. Los dos últimos llevan instrumentación de métricas.
- [x] **36. Consumir eventos desde Kafka.** Ambos jobs son consumidores independientes del mismo tópico, con `startingOffsets: earliest` y su propio `checkpointLocation`.
- [ ] **37. Validar estructura de eventos.** Hoy solo hay un `filter(id_cuenta IS NOT NULL)`, que **descarta en silencio** en lugar de validar y dar cuenta de lo descartado. Hace falta una validación explícita con un criterio documentado (tarea 17) y un registro de qué falla y por qué.
- [ ] **38. Separar eventos válidos y corruptos.** Bifurcar el flujo: los válidos a `bronze` y los corruptos a la DLQ. Es la contrapartida de la tarea 48 en el lado del procesamiento.
- [x] **39. Implementar escritura en Iceberg.** Tablas `demo.bronze.eventos_cuenta` y `demo.silver.transacciones_validadas` sobre el catálogo REST y MinIO. Incluye una función de evolución de esquema que añade columnas nuevas a tablas ya existentes sin reescribir datos.
- [x] **40. Construir capa `bronze`.** Guarda el histórico completo de transacciones, que PostgreSQL no conserva porque solo mantiene la última de cada cuenta. Verificado con 2.511 eventos persistidos.

### Fase 6 — Detección de anomalías ✅ **COMPLETA**

> **Entregable:** detección de anomalías en tiempo real. **Conseguido.** Queda calibrar el umbral, que es trabajo experimental de la fase 10.

- [x] **41. Definir método estadístico.** Z-score sobre el importe, por cuenta. Elegido por ser simple, explicable y justificable, frente a modelos de aprendizaje automático que quedan fuera del alcance.
- [x] **42. Implementar estadísticas por ventana.** Ventana de las últimas `VENTANA_LECTURAS` (20) transacciones de cada cuenta, calculada sobre el histórico de `bronze` en cada micro-lote.
- [x] **43. Calcular media.** Media del importe por cuenta dentro de la ventana.
- [x] **44. Calcular desviación.** Desviación típica muestral, con tratamiento explícito del caso de desviación nula o desconocida, que se da cuando una cuenta aún no tiene histórico suficiente.
- [x] **45. Definir umbral inicial mediante z-score.** `Z_SCORE_UMBRAL = 3.0`. Es un valor inicial razonable pero **sin calibrar con datos**: justificarlo empíricamente es trabajo de la fase 10.
- [x] **46. Marcar eventos normales/anómalos.** Cada transacción se escribe en `silver` con su media histórica, su desviación, su z-score y el booleano `es_anomalia`.
- [x] **47. Generar información suficiente para evaluar precisión y exhaustividad.** La etiqueta de verdad del generador viaja hasta `silver` y queda junto a la predicción, así que la matriz de confusión sale de una sola consulta. **Ninguna lógica de detección lee esa etiqueta**, para que el experimento siga midiendo algo.

### Fase 7 — Resiliencia y estrés

> **Entregable:** pipeline con mecanismos de resiliencia funcionando.

La instrumentación de medida (tareas 52 a 54) **ya está construida y probada**,
pero no se marca como hecha porque no se ha ejecutado ningún escenario de estrés:
lo que estas tareas piden es medir *bajo carga*, no tener con qué medir.

- [ ] **48. Implementar DLQ.** Un tópico Kafka dedicado a eventos fallidos, conservando el contenido original, el motivo del fallo y los metadatos necesarios para reprocesarlos. **Es la segunda aportación diferencial del TFG y hoy no existe nada.**
- [ ] **49. Implementar aislamiento de eventos corruptos.** Que un evento inválido acabe en la DLQ en vez de tumbar el job o desaparecer sin dejar rastro.
- [ ] **50. Comprobar que un evento corrupto no detiene el pipeline.** Prueba explícita: inyectar corrupción con el pipeline en marcha y verificar que el resto del flujo sigue procesándose con normalidad.
- [ ] **51. Implementar escenarios de pico de carga.** El modo de pico ya existe y se ha ejecutado una vez sobre el pipeline en marcha. Falta definir los escenarios concretos que entran en la evaluación y ejecutarlos de forma sistemática. **Antes hay que resolver el problema del techo del generador:** a 692 eventos/s la plataforma ni se inmuta, así que ese pico no llega a ser una prueba de estrés.
- [ ] **52. Medir consumer lag.** Instrumentado y ya **verificado como métrica útil**: con `MAX_OFFSETS_POR_TRIGGER=2000` y un trigger de 5 s, el lag arrancó en 35.504 eventos y bajó en escalones exactos de 2.000 hasta cero. Sin ese límite valdría 0 por construcción. Falta medirlo dentro de los escenarios de estrés de la evaluación.
- [ ] **53. Medir throughput.** Instrumentado y registrado por micro-lote. Falta medirlo en condiciones de saturación.
- [ ] **54. Medir latencia.** Calculable comparando `fecha_actualizacion` y `fecha_ingesta` en `bronze`. Falta medirla bajo carga y separando el transitorio de arranque del régimen estacionario.
- [ ] **55. Comprobar pérdida de eventos.** Comparar los eventos generados con los persistidos más los enviados a la DLQ. **La dependencia ya está resuelta:** el generador escribe al terminar un resumen JSON con el recuento exacto de lo emitido, que es el dato que PostgreSQL no puede dar al usar el patrón *account shadow*. Falta la comparación en sí, y que exista la DLQ.
- [ ] **56. Evaluar backpressure y capacidad de absorción.** Hasta dónde absorbe Kafka un pico sin que se pierda nada, y cómo se comporta Spark cuando el ritmo de llegada supera al de procesamiento. **Ya hay con qué provocarlo:** acotando el micro-lote se estrangula el consumo a voluntad, sin necesidad de acelerar el generador.

### Fase 8 — Transformaciones ELT

> **Entregable:** capas `silver` y `gold` funcionales y orquestadas. **Nada empezado.**

- [ ] **57. Configurar dbt Core.** Proyecto dbt con adaptador de Spark, conectado al catálogo Iceberg. Implica añadir un servicio nuevo al `docker-compose.yml`.
- [ ] **58. Definir modelos `silver`.** Hoy `silver` la escribe directamente el job de Spark. Habrá que decidir si se reorganiza para que dbt sea el propietario de esa capa o si `silver` se queda en streaming y dbt empieza en `gold`. **Es una decisión de diseño abierta.**
- [ ] **59. Definir modelos `gold`.** Agregados orientados al consumo: métricas por cuenta, por ubicación, por tipo de transacción y por ventana temporal.
- [ ] **60. Implementar limpieza.** Normalización de tipos, tratamiento de nulos y deduplicación de eventos CDC repetidos.
- [ ] **61. Implementar validaciones.** Tests de dbt sobre unicidad, no nulidad y rangos esperados.
- [ ] **62. Implementar agregaciones.** Las agregaciones que alimentan el panel y el informe de resultados.
- [ ] **63. Configurar Airflow.** Orquestador para las transformaciones periódicas. **Comprobar antes el consumo de memoria:** Airflow no es ligero y Spark ya ocupa más de la mitad de la memoria disponible.
- [ ] **64. Automatizar ejecuciones.** DAG que ejecute dbt de forma periódica, con supervisión y gestión de fallos.

### Fase 9 — Consulta y visualización

> **Entregable:** panel funcional. **Nada empezado.**

- [ ] **65. Configurar DuckDB.** Motor de consulta analítica ligero sobre las tablas Iceberg del lakehouse.
- [ ] **66. Elegir entre Metabase y Streamlit.** Decisión pendiente. Metabase da más por defecto pero pesa más; Streamlit es más ligero y más programable, y encaja mejor si el panel tiene que mostrar métricas del propio pipeline y no solo datos de negocio.
- [ ] **67. Crear panel de monitorización.** El panel que demuestra la plataforma en funcionamiento.
- [ ] **68. Mostrar anomalías.** Transacciones marcadas como sospechosas, con su z-score y su contexto.
- [ ] **69. Mostrar métricas.** Throughput, latencia y consumer lag, a partir de los CSV que ya genera la instrumentación.
- [ ] **70. Mostrar estado de la DLQ.** Cuántos eventos fallidos hay, por qué motivo y cuáles se han recuperado.
- [ ] **71. Mostrar información relevante del pipeline.** Estado de los servicios, volumen ingerido y retraso del procesamiento.

### Fase 10 — Evaluación experimental

> **Entregable:** informe de resultados. **Nada empezado, pero con la instrumentación lista.**

Esta es **la fase que responde a la pregunta de investigación**. Todo lo anterior
existe para hacerla posible.

- [ ] **72. Diseñar matriz de experimentos.** Qué combinaciones de carga y configuración se prueban, cuántas repeticiones y de qué duración. Debe ser limitada y reproducible: una matriz demasiado grande no cabe en el tiempo del TFG.
- [ ] **73. Definir diferentes niveles de carga.** Eventos por segundo, número de cuentas e intensidad de los picos. Punto de partida conocido: con 20 eventos/s el sistema va sobrado y el lag es 0.
- [ ] **74. Ejecutar pruebas de estrés.** Ejecución sistemática de la matriz, registrando la configuración exacta de cada corrida.
- [ ] **75. Modificar parámetros de configuración seleccionados.** Ya parametrizados y listos para barrer: `MAX_OFFSETS_POR_TRIGGER` e `INTERVALO_TRIGGER_SEGUNDOS` en los dos jobs, y en el generador el ritmo, la duración, la semilla y toda la configuración del pico. Quedan por parametrizar `VENTANA_LECTURAS` y `Z_SCORE_UMBRAL`, todavía fijos en el código, y por decidir si se estudian las particiones de Kafka (hoy 1).
- [ ] **76. Medir latencia.** Bajo cada combinación de la matriz.
- [ ] **77. Medir throughput.** Ídem.
- [ ] **78. Medir CPU.** Con `scripts/recolectar_recursos.py`, ya operativo.
- [ ] **79. Medir RAM.** Ídem.
- [ ] **80. Medir consumer lag.** Es la señal clave del experimento de pico de carga: mientras esté cerca de cero el sistema va al día; si crece, el procesamiento se está quedando por detrás.
- [ ] **81. Medir integridad de los datos.** Eventos generados frente a procesados, perdidos, enviados a la DLQ y recuperados. Depende de la tarea 55.
- [ ] **82. Analizar resultados.** Tratamiento de los CSV, gráficas y comparación entre configuraciones.
- [ ] **83. Extraer conclusiones.** La respuesta argumentada a la pregunta de investigación.

### Fase 11 — Memoria y defensa

> **Nada empezado, aunque hay bastante material ya acumulado.**

- [ ] **84. Redactar memoria.** El documento académico completo.
- [ ] **85. Incorporar resultados experimentales.** Depende de la fase 10.
- [ ] **86. Documentar decisiones de arquitectura.** **Buena parte ya está hecha:** la sección 6 de `CLAUDE.md` recoge cada decisión con sus alternativas y el motivo de cada descarte. Es material directamente reutilizable.
- [ ] **87. Documentar problemas y soluciones.** **También muy avanzado:** la sección 7 de `CLAUDE.md` documenta siete problemas reales con síntoma, causa, alternativas, solución elegida y verificación.
- [ ] **88. Preparar repositorio público.** Revisar que no queden credenciales en claro (hoy están escritas en `docker-compose.yml`, pendiente de pasarlas a un `.env`) ni ficheros ajenos al proyecto.
- [ ] **89. Preparar presentación.**
- [ ] **90. Preparar defensa.**

---

## 3. Qué hacer ahora

Por orden de prioridad:

1. **Modo de eventos corruptos y DLQ (tareas 31, 48, 49, 50).** La segunda aportación diferencial del TFG y lo único que queda de la fase 4. Antes hay que resolver la decisión de por dónde se inyecta la corrupción.
2. **Calibrar el detector (tarea 45).** Repetir la medición con `PROB_ANOMALIA=0.04` para obtener una exhaustividad que signifique algo.
3. **Parametrizar `VENTANA_LECTURAS` y `Z_SCORE_UMBRAL` (tarea 75).** Son las dos últimas constantes fijas en el código. Cinco minutos de trabajo que habilitan dos variables experimentales más.
4. **Diseñar la matriz de experimentos (tarea 72).** Ya están parametrizadas las palancas principales —ritmo de generación, intensidad del pico, tamaño del micro-lote e intervalo del trigger— así que la matriz ya se puede definir sobre variables que existen de verdad.
5. **Estado del arte y validación con el tutor (tareas 7 y 8).** No dependen del código y están en el camino crítico. Conviene avanzarlas en paralelo.

---

## 4. Decisiones pendientes

Decisiones que hay que tomar y que condicionan el trabajo posterior:

| Decisión | Afecta a | Estado |
|---|---|---|
| Por dónde se inyectan los eventos corruptos | Tareas 31, 48, 49 | **Abierta.** No es posible a través de PostgreSQL por su esquema tipado |
| Si dbt se hace dueño de `silver` o empieza en `gold` | Tareas 58, 59 | **Abierta** |
| Metabase o Streamlit | Tarea 66 | **Abierta** |
| Si Airflow y el panel caben en memoria | Tareas 63, 67 | **Por comprobar** antes de comprometerse |
| Pasar las credenciales a un `.env` | Tarea 88 | **Pendiente**, requerida por las convenciones del proyecto |

---

## 5. Referencias

- `CLAUDE.md` — memoria completa del proyecto: objetivos, decisiones con su justificación, problemas resueltos, resultados medidos y guía operativa de comandos.
- `README.md` — cómo levantar el entorno y ejecutar el pipeline.
- `documentation/propuesta_tfg_fraude_bancario.md` — propuesta inicial del TFG.

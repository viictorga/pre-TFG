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

## Fase 1 — Definición y estado del arte

- [ ] Definir enfoque principal en Ingeniería de Datos.
- [ ] Decidir que la plataforma será el protagonista del TFG.
- [ ] Definir una pregunta de investigación provisional.
- [ ] Establecer como requisito que todo el proyecto sea gratuito.
- [ ] Establecer Docker como entorno de despliegue.
- [ ] Confirmar el caso de uso (detección de fraude en transacciones bancarias simuladas) como definitivo.
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

- [ ] Configurar Docker Compose.
- [ ] Configurar PostgreSQL.
- [ ] Configurar Kafka o Redpanda.
- [ ] Configurar MinIO.
- [ ] Comprobar comunicación entre servicios.
- [ ] Documentar cómo levantar y detener el entorno.

**Entregable:** entorno de desarrollo funcional.

## Fase 4 — Generador de datos y CDC

- [ ] Implementar generador de datos de transacciones bancarias.
- [ ] Implementar generación de eventos normales.
- [ ] Implementar inyección controlada de anomalías.
- [ ] Implementar modo de pico de carga.
- [ ] Implementar modo de eventos corruptos.
- [ ] Configurar Debezium.
- [ ] Comprobar captura de INSERT/UPDATE.
- [ ] Comprobar publicación correcta en Kafka.

**Entregable:** datos sintéticos capturados como eventos CDC.

## Fase 5 — Streaming y capa Bronze

- [ ] Implementar Spark Structured Streaming.
- [ ] Consumir eventos desde Kafka.
- [ ] Validar estructura de eventos.
- [ ] Separar eventos válidos y corruptos.
- [ ] Implementar escritura en Iceberg.
- [ ] Construir capa `bronze`.

**Entregable:** pipeline streaming funcional hasta `bronze`.

## Fase 6 — Detección de anomalías

- [ ] Definir método estadístico.
- [ ] Implementar estadísticas por ventana.
- [ ] Calcular media.
- [ ] Calcular desviación.
- [ ] Definir umbral inicial mediante z-score.
- [ ] Marcar eventos normales/anómalos.
- [ ] Generar información suficiente para evaluar precisión y exhaustividad.

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

## Ramas

El repositorio utilizará únicamente **dos ramas principales de trabajo**:

- `main` → rama estable.
- `tfg` → rama de desarrollo del TFG.

Claude Code debe trabajar normalmente sobre `tfg` y **no debe trabajar directamente sobre `main`**.

No es necesario crear una rama diferente para cada fase o funcionalidad. Las distintas fases del proyecto se desarrollarán de forma progresiva dentro de `tfg`.

Si en algún momento fuese necesario crear una rama adicional por una circunstancia excepcional, debe consultarse previamente con el usuario.

## Merge

El usuario gestiona manualmente el merge de `tfg` a `main`.

Claude Code:

- NO debe hacer merge automáticamente.
- NO debe crear pull requests automáticamente.
- Debe dejar `tfg` limpia, probada y documentada para que el usuario revise y decida cuándo integrarla en `main`.

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
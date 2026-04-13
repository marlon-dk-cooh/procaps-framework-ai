# Triggers y automatizacion

Este documento explica **como se disparan los workflows** del template y diferencia dos niveles de automatizacion que a veces se confunden:

1. **Trigger del pipeline de CI/CD**  
   Define cuando Azure DevOps ejecuta el pipeline de despliegue.

2. **Trigger del Databricks Job**  
   Define cuando un job ya desplegado en Databricks se ejecuta automaticamente.

Esa distincion es importante porque:

- el pipeline despliega,
- el job procesa.

---

## 1) Dos niveles de trigger

### A. Trigger del pipeline (Azure DevOps)

Archivo relacionado:

- `workflow/Pipeline/workflow-pipeline.yml`

Este trigger decide cuando correr el pipeline que valida y despliega el bundle.

### B. Trigger del job (Databricks)

Archivos relacionados:

- `workflow/resources/indexing_workflow.yml`
- `workflow/resources/sharepoint_workflow.yml` (opcional)

Estos triggers deciden cuando ejecutar automaticamente un job ya desplegado en Databricks.

---

## 2) Estado actual del template

### Pipeline de Azure DevOps

El pipeline actual tiene trigger por rama configurado en:

```yaml
trigger:
  branches:
    include:
      - master
    exclude:
      - feature/*
```

Eso significa que hoy el disparo automatico del pipeline esta ligado a cambios en `master`.

Ademas, en la misma definicion hay seleccion condicional de variables:

- `feature/*` o `integration_dev` -> `lab-vars.yml`
- `main` -> `prd-vars.yml`

Esto implica que hay una desalineacion potencial entre:

- ramas que activan el pipeline,
- ramas que seleccionan variables,
- nombres reales de ambientes.

### Jobs de Databricks

Actualmente los jobs definidos en:

- `resources/indexing_workflow.yml`
- `resources/sharepoint_workflow.yml` (opcional)

**no tienen triggers automaticos configurados**.  
Se ejecutan manualmente o via CLI/API despues del despliegue.

---

## 3) Tipos de trigger disponibles en Databricks Jobs

Databricks soporta varios mecanismos de ejecucion automatica para jobs.

Los mas relevantes para este template son:

| Tipo | Para que sirve | Caso de uso tipico |
|---|---|---|
| `schedule` | Ejecutar por horario o cron | reindexacion nocturna, batch diario |
| `trigger.periodic` | Ejecutar cada cierto intervalo | polling simple cada N horas |
| `trigger.file_arrival` | Ejecutar cuando llegan archivos | indexacion automatica al subir documentos |
| `trigger.table_update` | Ejecutar cuando cambia una tabla | procesamiento de registros nuevos |
| `continuous` | Mantener el job corriendo siempre | escenarios de streaming continuo |
| API / CLI | Ejecucion bajo demanda | integracion con apps externas |

---

## 4) Cual trigger conviene para cada workflow

### Workflow de indexacion (`indexing_workflow_job`)

Recomendaciones:

- **`file_arrival`** si el origen son documentos que llegan a storage
- **`schedule`** si quieres reprocesar por lotes en una ventana fija
- **API/CLI** si otro sistema decide cuando indexar

## 5) Trigger manual o por API

Es la forma mas simple y la que ya funciona hoy sin cambios extra.

### Via Bundle CLI

```bash
databricks bundle run -t dev indexing_workflow_job
```

### Via Jobs API / CLI

```bash
databricks jobs run-now --job-id <JOB_ID> --profile workflow-dev
```

Usalo cuando:

- otro sistema decide cuando correr el flujo,
- quieres control total desde una app externa,
- aun no quieres dejar triggers permanentes activos.

---

## 6) Trigger por horario (`schedule`)

Usa este trigger cuando el workflow debe correr en horas fijas.

### Ejemplo

```yaml
resources:
  jobs:
    indexing_workflow_job:
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "America/Bogota"
        pause_status: UNPAUSED
```

### Cuando usarlo

- reindexacion diaria
- reprocesamiento nocturno

### Ventajas

- simple de operar
- facil de auditar
- ideal para cargas batch previsibles

### Consideraciones

- `schedule` no se combina con otros triggers del job
- si el job tarda mucho, debes revisar concurrencia y cola (`queue.enabled`)

---

## 7) Trigger periodico (`trigger.periodic`)

Usa este trigger cuando no necesitas cron exacto, solo un intervalo fijo.

### Ejemplo

```yaml
resources:
  jobs:
    indexing_workflow_job:
      trigger:
        periodic:
          interval: 6
          unit: HOURS
```

### Cuando usarlo

- polling de carpetas o fuentes externas
- sincronizacion recurrente
- refresco simple de un indice

### Consideraciones

- es mas simple que cron
- no define hora exacta de calendario

---

## 8) Trigger por llegada de archivos (`trigger.file_arrival`)

Es el trigger mas natural para workflows documentales.

### Como funciona

Databricks monitorea una ruta y dispara el job cuando detecta archivos nuevos.

### Ejemplo

```yaml
resources:
  jobs:
    indexing_workflow_job:
      trigger:
        file_arrival:
          url: "/Volumes/<catalog>/<schema>/<volume>/inputs/"
          min_time_between_triggers_seconds: 300
          wait_after_last_change_seconds: 60
```

### Parametros importantes

| Parametro | Significado |
|---|---|
| `url` | Ruta monitoreada |
| `min_time_between_triggers_seconds` | minimo tiempo entre ejecuciones |
| `wait_after_last_change_seconds` | espera para agrupar llegadas cercanas |

### Cuando usarlo

- indexacion automatica de documentos
- resumen automatico cuando entran archivos
- procesamiento de cargas por drop folder

### Recomendacion

Si activas este trigger, procura que la ruta monitoreada coincida con el origen configurado en el manifest.

---

## 9) Trigger por actualizacion de tabla (`trigger.table_update`)

Se usa cuando la fuente del flujo es una tabla Delta.

### Ejemplo

```yaml
resources:
  jobs:
    indexing_workflow_job:
      trigger:
        pause_status: UNPAUSED
        table_update:
          table_names:
            - "catalogo.schema.tabla_fuente"
          wait_after_last_change_seconds: 60
```

### Cuando usarlo

- clasificacion de registros nuevos
- enriquecimiento de tablas
- pipelines basados en cambios de datos estructurados

### Consideraciones

- aplica mejor a casos tipo ETL o eventos tabulares
- en este template tiene mas sentido para futuros casos de uso que para indexacion documental pura

---

## 10) Continuous

Mantiene el job en ejecucion continua.

### Ejemplo

```yaml
resources:
  jobs:
    indexing_workflow_job:
      continuous:
        pause_status: UNPAUSED
```

### Cuando usarlo

- streaming real
- procesamiento siempre activo

### Consideraciones

- no suele ser la opcion ideal para este template
- mantiene costos de compute constantes

---

## 11) Restricciones practicas

Para evitar configuraciones confusas:

- define **un solo mecanismo automatico** por job
- si estas empezando, usa primero ejecucion manual
- una vez validado el flujo, agrega `schedule` o `file_arrival`
- mantén alineado el trigger con el tipo de fuente:
  - archivos -> `file_arrival`
  - lotes programados -> `schedule`
  - tablas -> `table_update`

---

## 12) Como probar triggers en `dev`

### Opcion 1: prueba manual primero

```bash
databricks bundle deploy --profile workflow-dev -t dev
databricks bundle run --profile workflow-dev -t dev indexing_workflow_job
```

Si esto funciona, ya tienes base para automatizar.

### Opcion 2: prueba de `schedule`

Configura temporalmente un cron frecuente:

```yaml
schedule:
  quartz_cron_expression: "0 */5 * * * ?"
  timezone_id: "America/Bogota"
  pause_status: PAUSED
```

Despliegas, verificas en la UI y luego haces `Resume`.

### Opcion 3: prueba de `file_arrival`

1. configura `file_arrival`
2. despliega el job
3. activa el trigger en Databricks
4. sube un archivo a la ruta monitoreada
5. verifica que el job se dispare

---

## 13) Recomendacion operativa para este template

Orden sugerido de adopcion:

1. **Manual / CLI** durante desarrollo
2. **Schedule** para ambientes controlados
3. **File arrival** cuando la fuente documental ya sea estable
4. **API** si quieres integrarlo con otros sistemas

Para la mayoria de implementaciones iniciales:

- `indexing_workflow_job`: manual o `schedule`

---

## 14) Resumen

Este template puede automatizarse en dos niveles:

- **pipeline de despliegue**, controlado por Azure DevOps
- **ejecucion del job**, controlada por Databricks

Hoy el repo ya tiene pipeline de despliegue, pero los jobs no tienen triggers automaticos definidos en `resources/*.yml`.
Eso es una buena base para empezar: primero validar el flujo manualmente y luego activar el trigger mas adecuado para cada caso de uso.
# Triggers y Automatizacion

Los workflows de este template pueden ejecutarse de forma manual o automatica mediante triggers. Un trigger define **cuando** se dispara el job sin intervencion humana.

## Tipos de trigger disponibles

Databricks Jobs soporta un **unico trigger por job** (no se pueden combinar). Los tipos son:

| Tipo | Descripcion | Caso de uso tipico |
|------|-------------|--------------------|
| **File Arrival** | Se dispara al detectar archivos nuevos en un volumen UC o external location | Indexacion RAG: suben documentos y se indexan solos |
| **Schedule (Cron)** | Ejecucion periodica basada en expresion cron | Re-indexacion nocturna, reportes diarios |
| **Periodic** | Intervalo fijo simple (cada N /horas/dias) | Polling de datos, sincronizacion periodica |
| **Table Update** | Se dispara cuando una tabla Delta cambia | Procesar PQRs nuevas insertadas en una tabla |
| **Continuous** | El job corre siempre, se relanza al terminar | Procesamiento continuo de streaming |
| **API / Externo** | Se invoca desde sistemas externos via REST API | Integracion con Logic Apps, Event Grid, Power Automate |

---

## File Arrival

El trigger mas relevante para el caso de indexacion RAG. Databricks monitorea un volumen de Unity Catalog y cuando detecta archivos nuevos, lanza el job automaticamente.

### Como funciona

- Databricks revisa la ubicacion cada **~1 minuto** buscando archivos nuevos
- La revision es **recursiva** (incluye subdirectorios)
- Solo archivos **nuevos** disparan el trigger (sobrescribir un archivo existente no lo dispara)
- No genera costos adicionales mas alla del listado de archivos en el storage

### Configuracion

Se agrega en `resources/indexing_workflow.yml` al nivel del job (hermano de `tasks:`):

```yaml
resources:
  jobs:
    indexing_workflow_job:
      # ... tags, timeout, etc ...

      trigger:
        file_arrival:
          url: "/Volumes/inteligencia_artificial_transversal_qa/default/test_default/inputs/case4/"
          min_time_between_triggers_seconds: 300
          wait_after_last_change_seconds: 60

      tasks:
        # ... tareas del DAG ...
```

### Parametros

| Parametro | Descripcion | Recomendado |
|-----------|-------------|-------------|
| `url` | Ruta del volumen UC o external location a monitorear | La misma ruta de `raw_documents` del manifest |
| `min_time_between_triggers_seconds` | Tiempo minimo entre ejecuciones despues de que una termina | 300 (5 min) para evitar ejecuciones excesivas |
| `wait_after_last_change_seconds` | Tiempo de espera despues del ultimo archivo nuevo. Si llega otro archivo, el timer se reinicia | 60 para batches pequenos, 300 para uploads grandes |


### Limitaciones

- Sin file events habilitados en el external location:
  - Maximo **50 jobs** con file_arrival trigger por workspace
  - Maximo **10,000 archivos** en la ubicacion monitoreada
- Con file events habilitados: sin limites
- La ruta no puede contener wildcards (`*`, `?`)
- No detecta archivos sobrescritos, solo nuevos

---

## Schedule (Cron)

Ejecucion basada en un calendario. Usa expresiones [Quartz Cron](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html).

El formato Quartz es `segundos minutos horas día-mes mes día-semana`

### Configuracion

```yaml
resources:
  jobs:
    indexing_workflow_job:
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"    # Todos los dias a las 2:00 AM
        timezone_id: "America/Bogota"
        pause_status: UNPAUSED
```

### Expresiones cron comunes

| Expresion | Significado |
|-----------|-------------|
| `0 0 2 * * ?` | Todos los dias a las 2:00 AM |
| `0 0 */6 * * ?` | Cada 6 horas |
| `0 0 9 ? * MON-FRI` | Lunes a viernes a las 9:00 AM |
| `0 30 22 ? * SUN` | Domingos a las 10:30 PM |
| `0 0 0 1 * ?` | Primer dia de cada mes a medianoche |

### Nota

`schedule` y `trigger` son **mutuamente excluyentes**. Un job usa `schedule:` (cron) **o** `trigger:` (file_arrival / periodic / table_update), nunca ambos.

---

## Periodic

Version simplificada de schedule. Solo define un intervalo fijo.

### Configuracion

```yaml
resources:
  jobs:
    indexing_workflow_job:
      trigger:
        periodic:
          interval: 1
          unit: HOURS
```

### Unidades disponibles

`HOURS`, `DAYS`, `WEEKS`

---

## Table Update

Se dispara cuando una o mas tablas Delta reciben nuevos datos.
se debe configurar el tiempo de espera por enciam de los `60` segundos

### Configuracion

```yaml
resources:
  jobs:
    indexing_workflow_job:
      trigger:
        pause_status: UNPAUSED
        table_update:
          table_names:
            - "catalogo.schema.pqrs_pendientes"
            - "catalogo.schema.documentos_nuevos"
          condition: "status = 'pendiente'"
          wait_after_last_change_seconds: 60
```

### Parametros

| Parametro | Descripcion |
|-----------|-------------|
| `table_names` | Lista de tablas Delta a monitorear |
| `condition` | Condicion SQL opcional para filtrar (solo dispara si se cumple) |
| `wait_after_last_change_seconds` | Espera despues del ultimo cambio antes de disparar |

### Caso de uso

Ideal para flujos tipo clasificacion de PQRs: un sistema externo inserta registros en una tabla y el workflow se dispara para procesarlos.

---

## Continuous

El job se mantiene corriendo siempre. Cuando una ejecucion termina (exito o fallo), se lanza otra inmediatamente.

### Configuracion

```yaml
resources:
  jobs:
    indexing_workflow_job:
      continuous:
        pause_status: UNPAUSED
```

### Nota

Usar con precaucion: mantiene el cluster activo permanentemente, lo que genera costos continuos. Solo recomendado para procesamiento de streaming real.

---

## API / Invocacion externa

No requiere configuracion de trigger en el YAML. El job se invoca desde cualquier sistema externo.

### Via Databricks CLI

```bash
# Obtener el job ID
databricks jobs list --profile workflow-dev | grep indexing_workflow

# Ejecutar
databricks jobs run-now --job-id <JOB_ID> --profile workflow-dev
```

### Via REST API

```bash
curl -X POST "https://<workspace-host>/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"job_id": 12345}'
```

### Integraciones comunes

| Sistema | Como conectar |
|---------|---------------|
| Azure Logic Apps | Accion HTTP que llama al REST API de Databricks |
| Azure Event Grid | Suscripcion a eventos que invoca el endpoint |
| Power Automate | Conector HTTP o conector nativo de Databricks |
| Azure Data Factory | Actividad de Databricks en un pipeline ADF |
| Azure Functions | Funcion HTTP trigger que llama al API |

---


---

## Guia de pruebas en Dev

### Prueba 1: File Arrival

1. Configurar el trigger en `resources/indexing_workflow.yml` (u otro `resources/*.yml` del job) o en el override de dev en `databricks.yml`
2. Desplegar:
   ```bash
   databricks bundle deploy --profile workflow-dev -t dev
   ```
3. Ir a la UI de Databricks → **Workflows** → **Jobs** → tu job
4. En **Schedules & Triggers** verificar que aparece el trigger como **Paused**
5. Click en **Test connection** para validar que el path del volumen es correcto
6. Click en **Resume** para activar el trigger
7. Subir un archivo al volumen:
   ```bash
   databricks fs cp archivo_prueba.pdf \
     dbfs:/Volumes/inteligencia_artificial_transversal_qa/default/test_default/inputs/case4/ \
     --profile workflow-dev
   ```
8. Esperar ~1-2 minutos. El job debe arrancar automaticamente
9. Verificar en **Run history** que el job se ejecuto
10. Click en **Pause** para desactivar el trigger

### Prueba 2: Periodic

1. Cambiar la configuracion del trigger a periodic:
   ```yaml
   trigger:
     pause_status: PAUSED
     periodic:
       interval: 2
       unit: HOURS
   ```
2. Redesplegar: `databricks bundle deploy --profile workflow-dev -t dev`
3. En la UI, activar con **Resume**
4. Esperar 2 horas. El job debe arrancar solo
5. Verificar en **Run history**
6. **Pause** para detener

### Prueba 3: Schedule (Cron)

1. Cambiar la configuracion a schedule (nota: usa `schedule:` en vez de `trigger:`):
   ```yaml
   schedule:
     quartz_cron_expression: "0 */5 * * * ?"    # Cada 5 minutos (solo para prueba)
     timezone_id: "America/Bogota"
     pause_status: PAUSED
   ```
2. Redesplegar: `databricks bundle deploy --profile workflow-dev -t dev`
3. En la UI, activar con **Resume**
4. Esperar a que el cron se active (proximo minuto multiplo de 5)
5. Verificar en **Run history**
6. **Pause** para detener

### Prueba 4: Table Update

1. Cambiar la configuracion:
   ```yaml
   trigger:
     pause_status: PAUSED
     table_update:
       table_names:
         - "inteligencia_artificial_transversal_qa.transversal_gold.alguna_tabla_existente"
       wait_after_last_change_seconds: 30
   ```
2. Redesplegar: `databricks bundle deploy --profile workflow-dev -t dev`
3. En la UI, activar con **Resume**
4. Insertar datos en la tabla desde un notebook de Databricks
5. Esperar ~30 segundos. El job debe arrancar
6. **Pause** para detener

### Prueba 5: API

No requiere configuracion de trigger. Solo necesitas el job ID:

```bash
# Obtener el job ID del despliegue
databricks bundle summary --profile workflow-dev -t dev

# Ejecutar via API
databricks jobs run-now --job-id <JOB_ID> --profile workflow-dev
```

### Limpieza despues de las pruebas

Despues de probar todos los triggers, dejar la configuracion final deseada (o remover el trigger si no se necesita aun) y redesplegar:

```bash
databricks bundle deploy --profile workflow-dev -t dev
```

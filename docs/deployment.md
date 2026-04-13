# Despliegue

## Modelo de despliegue

El proyecto usa **Databricks Asset Bundles** como mecanismo de despliegue.
Un Asset Bundle empaqueta codigo fuente, configuracion y definiciones de infraestructura, y los despliega de forma consistente al workspace de Databricks.

```text
Repositorio local                      Databricks Workspace / APIs
─────────────────                      ────────────────────────────
src/steps/**/*.py                ──→   /Workspace/.../src/steps/**/*.py
config/*.yaml                    ──→   /Workspace/.../config/*.yaml
resources/indexing_workflow.yml  ──→   Definicion de Job indexing (API Jobs)
resources/sharepoint_workflow.yml ──→  Definicion de Job SharePoint → Bronze (API Jobs)
databricks.yml                   ──→   Cluster, variables, targets (Bundle API)
```

Internamente, Databricks genera artefactos Terraform en `.databricks/bundle/` para gestionar el estado de infraestructura del bundle.

---

## Jobs desplegados por el bundle

Con la configuracion actual (`databricks.yml` + `include`):

- `indexing_workflow_job` (indexacion: `s00_sharepoint` → `s00_discovery` … `s06_log_summary`)
- `sharepoint_workflow_job` (solo sincronizacion SharePoint → Bronze; script `src/steps/sharepoint/s00_sharepoint_to_bronze.py`)

Clusters por job (ver `targets.*.resources.jobs` en `databricks.yml`): por ejemplo `cluster_dedicado` (indexing), `cluster_sharepoint` (SharePoint).

---

## Ambientes y ramas (estado actual)

### Ambientes definidos en variables de pipeline

En `workflow/Pipeline/variables/` hoy existen:

- `lab-vars.yml`
- `prd-vars.yml`
- `general-vars.yml`

No existe archivo de variables `dev/dll` en esa carpeta.

### Ramas y seleccion de variables

En `workflow/Pipeline/workflow-pipeline.yml`:

- `feature/*` o `integration_dev`  -> carga `lab-vars.yml`
- `main` -> carga `prd-vars.yml`

### Trigger configurado actualmente

El trigger actual incluye solo:

- `master`

Esto significa que, con el YAML tal como esta hoy, la ejecucion automatica por push aplica solo a `master` (no a `feature/*`, `integration_dev` ni `main`, salvo ejecucion manual o ajuste de trigger).

---

## Proceso de despliegue recomendado

## 1) Validar bundle

```bash
databricks bundle validate
```

## 2) Desplegar bundle

```bash
databricks bundle deploy -t dev
```

Nota: el target disponible en `databricks.yml` es `dev`.  
Si necesitas `lab/prd` como targets reales de bundle, deben existir definidos en `databricks.yml`.

## 3) Ejecutar jobs

```bash
# Job de indexacion
databricks bundle run -t dev indexing_workflow_job

# Job solo SharePoint -> Bronze (manifest sharepoint_manifest.yaml)
databricks bundle run -t dev sharepoint_workflow_job
```

---

## Despliegue manual (desarrollador)

Para iterar rapido en desarrollo:

1. autenticar perfil Databricks local
2. validar
3. desplegar a `-t dev`
4. ejecutar job y revisar logs en Databricks UI

Comandos tipicos:

```bash
databricks auth env --profile <tu-profile>
databricks bundle validate --profile <tu-profile>
databricks bundle deploy --profile <tu-profile> -t dev
databricks bundle run --profile <tu-profile> -t dev indexing_workflow_job
```

---

## Despliegue por CI/CD (Azure DevOps)

El pipeline de `workflow/Pipeline/workflow-pipeline.yml` extiende un template central:

- `pipeline-templates-framework-ia2/pipelines/main.yml@template`

y define `language: Databricks`.

El Service Principal se usa para despliegue automatizado cuando el pipeline corre en Azure DevOps.

---

## Monitoreo de ejecucion

### En Databricks UI

- **Workflows > Jobs**
- revisar historial de runs
- revisar DAG por task
- revisar logs por task

### En almacenamiento

Los steps escriben logs de ejecucion en `silver/logs/<run_id>/...` y consolidan resumen en `gold/logs/<run_id>/...` (segun el workflow).

---

## Troubleshooting rapido

### `bundle validate` falla

Causas comunes:

- `python_file` con ruta incorrecta en `resources/*.yml`
- YAML invalido
- variables referenciadas no definidas

### Job falla en una task

1. abrir run en Databricks UI
2. entrar a la task fallida
3. revisar `stdout/stderr`
4. validar conectividad/secretos/endpoints del `config.yaml`

### El cluster no arranca

- tipo de nodo no disponible
- restricciones de policy
- cuotas insuficientes en Azure

---

## Recomendaciones de alineacion (importante)

Para evitar confusiones entre ramas, variables y targets:

1. alinear trigger con ramas reales (`main` vs `master`)
2. definir si habra ambiente `dev/dll` en pipeline y crear su archivo de variables
3. alinear nombres de ambiente entre:
   - `Pipeline/variables/*.yml` (`BUNDLE_TARGET`)
   - `databricks.yml` (`targets`)

Cuando esas tres piezas coinciden, el despliegue queda predecible y repetible.
# Despliegue

## Modelo de despliegue

El proyecto usa **Databricks Asset Bundles** como mecanismo de despliegue. Un Asset Bundle empaqueta codigo fuente, configuracion y definiciones de infraestructura, y los despliega atomicamente al workspace de Databricks.

```
Repositorio local                    Databricks Workspace
─────────────────                    ────────────────────
src/steps/*.py                  ──→   /Workspace/.../src/steps/*.py
config/*.yaml                   ──→   /Workspace/.../config/*.yaml
resources/indexing_workflow.yml ──→   Job indexacion (API)
resources/sharepoint_workflow.yml ──→ Job SharePoint -> Bronze (API)
databricks.yml                  ──→   Cluster config, permissions (API)
```

Internamente, Databricks genera archivos Terraform en `.databricks/bundle/` para gestionar el estado de la infraestructura.

## Ambientes y ramas

El proyecto maneja tres ambientes con un flujo de ramas bien definido:

| Ambiente | Branch | Quien despliega | Workspace |
|----------|--------|-----------------|-----------|
| **Dev** | Cualquiera (rama de feature/local) | Desarrollador (manual) | Dev workspace |
| **Integration** | `Integration` | Service Principal (CI/CD) | Dev/QA workspace |
| **Produccion** | `master` | Service Principal (CI/CD) | Prod workspace |

### Flujo de ramas y CI/CD

```
feature/* ──→ Integration ──→ master
   │              │               │
   ▼              ▼               ▼
  Dev          CI/CD           CI/CD
(manual)    (automatico)    (automatico)
```

1. **Dev (manual)**: El desarrollador trabaja en su rama, despliega con su perfil de Databricks a su workspace personal, ejecuta y revisa manualmente.
2. **Integration (CI/CD)**: Al hacer merge a la rama `Integration`, el pipeline de CI/CD se dispara automaticamente. El **Service Principal** ejecuta:
   - Validacion de dependencias
   - Validacion de APIs hardcodeadas
   - Cobertura de codigo
   - Despliegue automatico al workspace de integracion
3. **Produccion (CI/CD)**: Al hacer merge a `master`, el mismo pipeline de CI/CD corre las validaciones y el **Service Principal** despliega a produccion.

### Diferencias clave por ambiente

| Aspecto | Dev | Integration / Prod |
|---------|-----|-------------------|
| Root path | `/Workspace/Users/<usuario>/.bundle/<nombre>/dev` | `/Shared/AI_Workflows/<dominio>/<nombre>` |
| Quien despliega | Desarrollador con perfil personal | Service Principal via CI/CD |
| Validaciones | Manual (`bundle validate`) | Automaticas (deps, APIs, cobertura) |
| Instancias | ON_DEMAND_AZURE | ON_DEMAND |
| Auto-terminacion | 5 min | 30 min |
| Permisos | Usuario actual | Grupos AD configurados |

## Proceso de despliegue

### Despliegue manual a Dev (desarrollador)

El desarrollador tiene su propio perfil de Databricks y puede desplegar y ejecutar manualmente para iterar rapido:

```bash
# 1. Verificar autenticacion
databricks auth env --profile workflow-dev

# 2. Validar configuracion
databricks bundle validate --profile workflow-dev 

# 3. Desplegar a su workspace personal
databricks bundle deploy --profile workflow-dev -t dev

# 4. Verificar
databricks bundle summary --profile workflow-dev -t dev
```

El desarrollador puede ir a la UI de Databricks de su workspace para revisar los resultados, logs y metricas de cada ejecucion.

### Despliegue a Integration (CI/CD automatico)

Al hacer merge a la rama `Integration`, el pipeline de CI/CD se ejecuta automaticamente:

1. **Validacion de dependencias**: verifica que todas las librerias PyPI estan disponibles y son compatibles
2. **Validacion de APIs hardcodeadas**: detecta endpoints o credenciales quemadas en el codigo
3. **Cobertura de codigo**: ejecuta tests y verifica umbrales de cobertura
4. **Despliegue**: el Service Principal ejecuta `databricks bundle deploy -t dev` (o target de integracion)

No se requiere intervencion manual. Si alguna validacion falla, el pipeline se detiene y notifica al equipo.

### Despliegue a Produccion (CI/CD automatico)

Al hacer merge a `master`, el mismo pipeline de CI/CD se ejecuta con el target de produccion:

1. Las mismas validaciones que en Integration
2. El Service Principal ejecuta `databricks bundle deploy -t prod`
3. Despliega al workspace de produccion con permisos de grupo AD

## Ejecucion del workflow

### Por CLI

```bash
databricks bundle run --profile workflow-dev -t dev indexing_workflow_job
```

### Por UI de Databricks

1. Ir a **Workflows > Jobs** en el workspace
2. Buscar el job de indexacion (nombre tipico `${bundle.name}-indexing_job`, segun `resources/indexing_workflow.yml`)
3. Click en **Run Now**

### Por trigger

El job soporta `max_concurrent_runs: 3` y tiene `queue.enabled: true`, lo que permite:

- **Ejecucion por API**: llamar al endpoint de Databricks Jobs Run
- **Ejecucion por schedule**: configurar un cron schedule en la definicion del job
- **Ejecucion por evento**: conectar con Azure Event Grid o Logic Apps

## Monitoreo de ejecucion

### Desde CLI

```bash
# Ver ejecuciones recientes
databricks jobs list-runs --job-id <JOB_ID> --profile workflow-dev
```

### Desde UI

En la pagina del job en Databricks:

- **Run history**: historial de ejecuciones con estado
- **Task DAG**: visualizacion grafica del DAG con estado por tarea
- **Logs**: logs de cada tarea individual
- **Metrics**: metricas de duracion y recursos

### Metricas internas

Cada step registra metricas y alertas a traves del `ExecutionTracker`. Estos datos quedan en tablas de Unity Catalog y son consultables para monitoreo personalizado.

## Gestion del cluster

El job usa un **cluster efimero compartido** (`cluster_temp_ia`):

- Se crea automaticamente al iniciar el job
- Se comparte entre todas las tareas del DAG
- Se destruye tras `autotermination_minutes` de inactividad
- Tiene Spark configs optimizadas para Delta:
  - `spark.databricks.delta.preview.enabled`: true
  - `spark.databricks.delta.optimizeWrite.enabled`: true
  - `spark.sql.adaptive.enabled`: true

## Troubleshooting

### El bundle no valida

```bash
databricks bundle validate --profile workflow-dev --output json
```

Revisar el JSON de salida para errores especificos. Los mas comunes:
- Referencia a archivos que no existen (python_file path)
- Variables sin valor por defecto
- Sintaxis YAML invalida

### Una tarea falla

1. Ir al job en la UI de Databricks
2. Click en la ejecucion fallida
3. Click en la tarea especifica que fallo
4. Revisar **Logs > Standard output** y **Standard error**

Las alertas P1 y P2 del `ExecutionTracker` indican problemas criticos. Cada step loguea errores con `exc_info=True` para trazabilidad completa.

### El cluster no inicia

- Verificar que el `policy_id` referenciado en `databricks.yml` exista en el workspace
- Verificar que el tipo de nodo (`node_type_id`) este disponible en la region
- Verificar cuotas de vCPU en la suscripcion de Azure

### Timeouts

- Timeout por tarea: 1800 segundos (30 min)
- Timeout total del job: 7200 segundos (2 horas)
- Cada tarea tiene `max_retries: 1` con `retry_on_timeout: true`

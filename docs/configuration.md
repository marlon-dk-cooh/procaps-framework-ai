# Configuracion de workflows (`workflow`)

Este documento explica como se configuran los workflows asincronos del repositorio:

- **Workflow de indexacion** (job `indexing_workflow_job`)
- **Workflow SharePoint → Bronze** (job `sharepoint_workflow_job`, independiente del indexing)

El objetivo es tener claro **que archivo controla que cosa**, en que orden se leen y como se relacionan.

---

## 1) Mapa rapido de archivos de configuracion

```
./
├── databricks.yml
├── resources/
│   ├── indexing_workflow.yml         # Job de indexacion (DAG s00_sharepoint → s00_discovery..s06)
│   └── sharepoint_workflow.yml       # Job solo SharePoint → Bronze (s00_sharepoint_to_bronze)
├── config/
│   ├── config.yaml                   # Config funcional compartida (servicios, parametros)
│   ├── indexing_manifest.yaml        # Contrato logico indexacion
│   └── sharepoint_manifest.yaml      # Contrato job SharePoint -> Bronze standalone (sharepoint_workflow_job)
└── src/steps/
    ├── indexing/
    └── sharepoint/
```

---

## 2) Como leer la configuracion por capas

La configuracion se entiende mejor en 3 capas:

1. **Capa de despliegue (Bundle):** `databricks.yml`  
   Define targets, workspace, variables y cluster de job.

2. **Capa de orquestacion (Jobs):** `resources/*.yml`  
   Define tasks, dependencias, timeouts, retries, librerias y scripts Python.

3. **Capa funcional/datos:** `config/*.yaml`  
   Define endpoints, secretos, parametros de procesamiento y contrato input/output por step.

---

## 3) `databricks.yml` (Bundle y entorno)

`databricks.yml` es el punto de entrada de despliegue.

### 3.1 Include de jobs

En `databricks.yml` se incluyen:

- `resources/indexing_workflow.yml`
- `resources/sharepoint_workflow.yml`

### 3.2 Variables globales

Variables mas importantes:

- `business_domain`
- `node_type_id`
- `num_workers`
- `availability`
- `autotermination_minutes`
- `spark_version`

Estas variables se consumen en la definicion de clusters de job (por ejemplo `cluster_dedicado` para `indexing_workflow_job`).

### 3.3 Targets

En `targets.dev` se define:

- `workspace.host`
- `workspace.root_path`
- overrides de variables por ambiente

### 3.4 Cluster

El cluster de jobs de indexacion se define en:

- `targets.dev.resources.jobs.indexing_workflow_job.job_clusters[...]`

Nota: el job de indexacion define su cluster (por ejemplo `cluster_dedicado`) en el target de `databricks.yml`.

---

## 4) `resources/indexing_workflow.yml` (Job de indexacion)

Define el job asincrono de indexacion (`indexing_workflow_job`). El DAG arranca con **`s00_sharepoint`** (sync real solo si `config` lo permite; ver §5.3) y **`s00_discovery`** depende de ese task.

### 4.1 Tasks

- `s00_sharepoint` (sincroniza SharePoint → Bronze bajo el mismo `path_prefix` que `s00_discovery` si `config.indexing.sharepoint_sync_enabled` es `true`)
- `s00_discovery`
- `s01_textual_ingestion`
- `s01_visual_ingestion`
- `s01_tabular_ingestion`
- `s01_structured_ingestion`
- `s02_chunking`
- `s02_image_captioning`
- `s03_sanitization`
- `s04_embeddings_keywords`
- `s05_indexing`
- `s06_log_summary`

### 4.2 Scripts llamados

Todos apuntan al namespace de indexacion, por ejemplo:

- `../src/steps/indexing/s00_sharepoint.py`
- `../src/steps/indexing/s00_discovery.py`
- ...
- `../src/steps/indexing/s06_log_summary.py`

El task `s00_sharepoint` incluye dependencias PyPI alineadas con el cliente SharePoint (`Office365-REST-Python-Client`, almacenamiento, Key Vault).

### 4.3 Parametros CLI estandar por task

Cada task recibe:

- `--config ${workspace.file_path}/config/config.yaml`
- `--manifest ${workspace.file_path}/config/indexing_manifest.yaml`
- `--run_id {{job.run_id}}`
- `--task_run_id {{task.run_id}}`

---

## 5) `config/config.yaml` (config funcional compartida)

Es la configuracion transversal que consumen los jobs del bundle (indexacion y SharePoint standalone).

### 5.1 Secciones principales

- `global_settings`: ambiente y logging
- `pipeline`: identidad del pipeline
- `secrets`: proveedor de secretos (Key Vault)
- `llms`: modelos LLM
- `embeddings`: modelo de embeddings
- `security`: PII y content safety
- `vector_stores`: Azure AI Search (endpoint, key, index, dimensiones)
- `processing.chunking`: estrategia de chunking
- `ocr`: servicio OCR
- `indexing`: activacion del paso SharePoint dentro del job de indexacion (ver abajo)
- `sharepoint`: conexion a SharePoint Online y destino Bronze por defecto (tambien usada por `s00_sharepoint` en indexing cuando esta habilitada)

### 5.2 Uso por workflow

- **Indexacion** usa casi todas las secciones (OCR, chunking, embeddings, search, seguridad) y, si sincronizas desde SharePoint antes del discovery, **`indexing`** y **`sharepoint`** en `config.yaml`.
- **SharePoint standalone** reutiliza **`sharepoint.*`** (y el mismo mecanismo de resolución de secretos `${KV-...}`).

### 5.3 Indexacion: SharePoint antes del discovery (`indexing` + `sharepoint`)

El task `s00_sharepoint` **siempre** existe en `indexing_workflow_job`, pero la sincronizacion real solo corre si:

| Clave | Ubicacion | Efecto |
|-------|-----------|--------|
| `sharepoint_sync_enabled` | `config.yaml` → `indexing.sharepoint_sync_enabled` | Debe ser **`true`** para descargar desde SharePoint y subir a Bronze en el prefijo alineado con `s00_discovery` (`path_prefix` en `indexing_manifest.yaml`). Si es `false` o falta, el step termina en no-op y deja constancia en el log en Silver. |
| `enabled` | `config.yaml` → `sharepoint.enabled` | Si es `false`, tampoco se sincroniza (deshabilitacion global del modulo SharePoint). |

Credenciales y rutas de biblioteca SharePoint: mismos campos que el workflow dedicado (`site_url`, `client_id`, `client_secret`, `relative_path`, etc. en `sharepoint.*`). El prefijo bajo el contenedor ADLS `bronze` para el flujo de indexacion lo fuerza el codigo al valor de **`workflow.steps.s00_discovery.inputs` → `raw_documents.path_prefix`**, para coincidir con el escaneo de `s00_discovery`.

Opciones de ejecucion del sync (`max_folders`, `only_folder`, `since_date`): `indexing_manifest.yaml` → `workflow.steps.s00_sharepoint.options`.

---

## 6) `config/indexing_manifest.yaml` (contrato de indexacion)

Este archivo sustituye al nombre historico `workflow_manifest.yaml` (mismo contenido y clave raiz `workflow:`). Tras renombrar, vuelve a desplegar el bundle para que los tasks apunten al path nuevo.

Define el contrato logico del DAG de indexacion:

- `catalog` y `schema`
- `workflow.steps.<step>.inputs`
- `workflow.steps.<step>.outputs`
- `workflow.steps.s00_sharepoint.options`: parametros del proceso SharePoint → Bronze **dentro del job de indexacion** (misma semantica que el workflow standalone; ver §7 y `docs/architecture_sharepoint.md`)

### Opciones de `s00_sharepoint` (`workflow.steps.s00_sharepoint.options`)

Claves soportadas por `src/steps/indexing/s00_sharepoint.py` (todas opcionales):

| Clave | Tipo / uso |
|-------|------------|
| `max_folders` | Entero `> 0`: tope de carpetas a recorrer. Si falta o es `null`, no hay limite. |
| `only_folder` | Cadena: acota a una carpeta (biblioteca/sitio segun `SharePointToBronzeProcess`). Si falta o es `null`, se procesan todas las rutas aplicables. |
| `since_date` | ISO 8601 (por ejemplo `2026-01-01T00:00:00` o sufijo `Z`). Si falta o es `null`, no se filtra por fecha de modificacion. |

En el repositorio, la plantilla suele listar las tres claves con valor `null` para documentar el contrato; **`null` equivale a omitir la clave** (el codigo trata ambos casos igual). Tambien puedes usar `options: {}` si no necesitas filtros.

El prefijo bajo el contenedor `bronze` **no** sale de estas opciones: lo fija `s00_discovery.inputs.raw_documents.path_prefix` (alineacion con el discovery).

---

## 7) `config/sharepoint_manifest.yaml` (contrato del job SharePoint standalone)

Lo consume **`sharepoint_workflow_job`** (`resources/sharepoint_workflow.yml`), script `src/steps/sharepoint/s00_sharepoint_to_bronze.py`, con `--manifest .../config/sharepoint_manifest.yaml`.

- Define el contrato logico del DAG de ese job (catalog/schema, steps, inputs/outputs).
- Las opciones de ejecucion (`max_folders`, `only_folder`, `since_date`) van bajo el step correspondiente del manifiesto (por ejemplo `workflow.steps.s00_sharepoint_to_bronze.options`), no en `indexing_manifest.yaml`.

Credenciales y biblioteca SharePoint siguen en `config/config.yaml` → `sharepoint.*`. Detalle en `docs/architecture_sharepoint.md`.

---

## 8) Relacion entre indexing y SharePoint standalone

Los jobs declarados en `databricks.yml` comparten el mismo bundle y, en general, **`config/config.yaml`** (secretos, `sharepoint.*`, modelos, etc.). Cada uno tiene **su YAML de orquestacion** en `resources/` y **su manifiesto** cuando aplica:

| Job | Resource | Manifiesto principal |
|-----|----------|----------------------|
| `indexing_workflow_job` | `indexing_workflow.yml` | `indexing_manifest.yaml` |
| `sharepoint_workflow_job` | `sharepoint_workflow.yml` | `sharepoint_manifest.yaml` |

Los namespaces de steps estan separados (`src/steps/indexing`, `src/steps/sharepoint`), de modo que se pueda evolucionar un flujo sin romper el otro.

---

## 9) Orden recomendado para configurar por primera vez

1. Ajustar `databricks.yml`:
   - `workspace.host`
   - `workspace.root_path`
   - variables de cluster

2. Ajustar `config/config.yaml`:
   - endpoints
   - secretos
   - indice de search
   - parametros de procesamiento

3. Ajustar `config/indexing_manifest.yaml` (indexacion):
   - catalog/schema
   - inputs/outputs por step

4. Si usas el job solo SharePoint: ajustar `config/sharepoint_manifest.yaml` y validar `sharepoint.*` en `config.yaml` (ver §7).

5. Validar y desplegar:
   - `databricks bundle validate`
   - `databricks bundle deploy`

6. Ejecutar jobs:
   - indexacion: `indexing_workflow_job`
   - (opcional) SharePoint standalone: `sharepoint_workflow_job`

---

## 10) Buenas practicas de mantenimiento

- Mantener sincronizados:
  - task keys en `resources/*.yml` (incluido `s00_sharepoint` → `s00_discovery` en `indexing_workflow.yml`)
  - `step_name` en scripts Python
  - nodos en manifests
- Evitar duplicar configuracion por workflow cuando puede vivir en `config.yaml`.
- Versionar cambios de schema de Search junto con cambios en `s05_indexing.py`.
- Si cambias rutas de scripts, actualizar `python_file` en resources inmediatamente.

Con esta estructura, la configuracion queda clara, auditable y preparada para crecer a mas workflows asincronos.
# Checklist minima antes del primer deploy

## 1) `databricks.yml`

- [ ] `targets.dev.workspace.host`: poner la URL real de tu workspace de Databricks.
- [ ] `targets.dev.workspace.root_path`: definir la ruta donde se desplegara el bundle en el workspace.
- [ ] `bundle.name`: validar nombre final del bundle (evita conflictos con otros jobs).
- [ ] `variables.business_domain`: ajustar al dominio de negocio real.
- [ ] `targets.dev.variables.node_type_id`: confirmar tipo de nodo disponible en tu workspace.
- [ ] `targets.dev.variables.num_workers`: definir cantidad minima necesaria para el flujo.
- [ ] `targets.dev.variables.spark_version`: validar que esa version este habilitada en tu entorno.
- [ ] `targets.dev.resources.jobs.indexing_workflow_job.job_clusters[*].new_cluster.spark_env_vars`: revisar variables de entorno requeridas (si usas indice privado o identidades administradas).

## 2) `config/indexing_manifest.yaml`

- [ ] `workflow.catalog`: cambiar al catalogo real de tu entorno.
- [ ] `workflow.schema`: cambiar al schema real de salida.
- [ ] `workflow.steps.s00_discovery.inputs[0]`: definir la fuente real de entrada (`type`, `container_name`, `path_prefix` en ADLS Bronze para el discovery).
- [ ] Si usas sync SharePoint en indexacion: revisar `workflow.steps.s00_sharepoint.options` (`max_folders`, `only_folder`, `since_date`; `null` u omitir = sin ese filtro) y que `path_prefix` de discovery sea el prefijo donde debe caer el contenido desde SharePoint.
- [ ] Revisar nombres de outputs por step (`ir_data`, `chunks_data`, `embeddings_data`, etc.) para alinearlos con tus convenciones.
- [ ] Confirmar que la secuencia de dependencias entre steps coincide con tu caso de uso (si agregas/quitas steps, actualiza inputs/outputs).

## 3) `config/sharepoint_manifest.yaml` (solo si usas `sharepoint_workflow_job`)

- [ ] Ajustar catalog/schema y steps segun tu entorno.
- [ ] Revisar opciones del step SharePoint → Bronze (`max_folders`, `only_folder`, `since_date`) y coherencia con `sharepoint.*` en `config.yaml`.

## 4) `config/config.yaml`

- [ ] Configurar endpoints/servicios (OCR, embeddings, search) segun tu ambiente.
- [ ] Configurar nombres de indice y tablas destino.
- [ ] Verificar rutas/prefijos de almacenamiento (bronze/silver/gold o volumenes UC).
- [ ] Ajustar parametros funcionales clave (chunk size, overlap, batch size, retries/timeouts).
- [ ] Verificar referencia a secretos/Key Vault (nombres de secretos y alcance).
- [ ] Confirmar que no queden valores placeholder para ambientes de ejemplo.
- [ ] Indexacion con SharePoint: `indexing.sharepoint_sync_enabled` y seccion `sharepoint` (sitio, credenciales, `enabled`).

## 5) Validacion rapida previa

- [ ] Ejecutar `databricks bundle validate` y corregir cualquier error de sintaxis/referencias.
- [ ] Ejecutar un primer `databricks bundle deploy` en `dev`.
- [ ] Lanzar `databricks bundle run` del job `indexing_workflow_job` y validar en Jobs que corran `s00_sharepoint` (no-op o sync), `s00_discovery` a `s06_log_summary`.
- [ ] (Opcional) Probar `sharepoint_workflow_job` si dependes del sync standalone.
- [ ] Revisar artefactos generados (tablas/indice/log summary) para confirmar que la configuracion minima quedo correcta.

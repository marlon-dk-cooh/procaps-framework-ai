# Personalizacion del template

Este template esta diseñado para ser adaptado a diferentes casos de uso.
El ejemplo incluido (indexacion RAG) demuestra el patron completo, pero la estructura es generica para cualquier flujo asincrono en Databricks Jobs.

---

## 1) Que se puede personalizar

| Componente | Archivo(s) | Que cambiar |
|---|---|---|
| Orquestacion del workflow | `resources/*.yml` | Tasks, dependencias, timeouts, retries, librerias (`indexing_workflow.yml`, `sharepoint_workflow.yml`, plantilla `new_workflow.yml`) |
| Contrato logico del DAG | `config/*_manifest.yaml` | Inputs, outputs, tipos, medallion, catalog/schema |
| Logica de steps | `src/steps/<caso>/` | Crear o adaptar scripts Python por step |
| Config funcional | `config/config.yaml` | Endpoints, modelos, chunking, seguridad, search, OCR |
| Infraestructura y despliegue | `databricks.yml` | Bundle name, targets, workspace, variables, cluster |

---

## 2) Estructura base recomendada

Para mantener separacion por dominio/caso de uso:

```text
<raíz del repo>/
├── resources/
│   ├── indexing_workflow.yml           # indexación RAG
│   ├── sharepoint_workflow.yml         # SharePoint → Bronze
│   ├── new_workflow.yml                # plantilla de job (copiar a <nuevo_caso>_workflow.yml)
│   └── <nuevo_caso>_workflow.yml      # instancia de un nuevo caso
├── config/
│   ├── config.yaml
│   ├── indexing_manifest.yaml
│   ├── sharepoint_manifest.yaml
│   ├── new_manifest.yaml               # plantilla de manifest
│   └── <nuevo_caso>_manifest.yaml
└── src/steps/
    ├── indexing/
    ├── sharepoint/
    └── new_workflow/                   # plantilla de steps (copiar a <nuevo_caso>/)
```

En `resources/`, `new_workflow.yml` es la plantilla del job (no está incluida en `databricks.yml` hasta que la copies); en `config/`, `new_manifest.yaml`; en `src/steps/new_workflow/`, scripts de ejemplo (`s00_ingestion.py`, `s01_preprocess.py`). Copia y renombra al crear un caso real, añade el YAML del job al `include:` de `databricks.yml` y define `job_clusters` para ese job en el `target` (véase los bloques existentes para `indexing_workflow_job` o `sharepoint_workflow_job`).

---

## 3) Como agregar un nuevo caso de uso (paso a paso)

### Paso 1: Definir el DAG

Diseña la secuencia de steps y sus dependencias.

Ejemplo:

```text
s00_ingestion -> s01_preprocess -> s02_enrich -> s03_publish
```

Si tienes ramas paralelas, definelas desde este punto.

### Paso 2: Actualizar (o crear) el manifest

Crea `config/<nuevo_caso>_manifest.yaml` con el contrato de datos del DAG:

```yaml
workflow:
  catalog: "main"
  schema: "mi_caso_schema"
  steps:
    s00_ingestion:
      inputs:
        - key: "raw_input"
          type: "container"
          container_name: "bronze"
      outputs:
        - key: "ingested_data"
          type: "file"
          medallion: "silver"

    s01_preprocess:
      inputs:
        - from_step: "s00_ingestion"
          key: "ingested_data"
          type: "file"
      outputs:
        - key: "clean_data"
          type: "file"
          medallion: "silver"
```

### Paso 3: Crear scripts de steps en `src/steps`

Crea una carpeta dedicada:

- `src/steps/<nuevo_caso>/`

y dentro los scripts (`s00_*.py`, `s01_*.py`, etc.) con patron estandar:

- parse de argumentos (`--config`, `--manifest`, `--run_id`, `--task_run_id`)
- `load_config` + `load_manifest`
- `StepContext`
- `process(step_ctx)` con `StepLogger`

Plantilla minima:

```python
import argparse
from src.core.settings import load_config, load_manifest
from src.core.pipeline_context import StepContext
from src.core.logging_config import get_logger

logger = get_logger(__name__)

def process(step_ctx: StepContext):
    from src.core.run_logger import StepLogger
    with StepLogger(step_ctx) as sl:
        logger.info(f"Starting Step: {step_ctx.step_name}")
        # logica del step
        sl.set_input_count(0)
        sl.set_output_count(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--task_run_id", required=True)
    args = parser.parse_args()

    config = load_config(args.config)
    manifest = load_manifest(args.manifest)
    step_ctx = StepContext(config, manifest, args.run_id, args.task_run_id, step_name="s00_ingestion")
    process(step_ctx)
```

### Paso 4: Definir tareas en el workflow

Crea `resources/<nuevo_caso>_workflow.yml` (plantilla de partida: `resources/new_workflow.yml`):

```yaml
resources:
  jobs:
    nuevo_caso_workflow_job:
      name: "${bundle.name}-nuevo_caso_workflow_job"
      tasks:
        - task_key: s00_ingestion
          job_cluster_key: "cluster_dedicado"
          spark_python_task:
            python_file: "../src/steps/<nuevo_caso>/s00_ingestion.py"
            parameters:
              - "--config"
              - "${workspace.file_path}/config/config.yaml"
              - "--manifest"
              - "${workspace.file_path}/config/<nuevo_caso>_manifest.yaml"
              - "--run_id"
              - "{{job.run_id}}"
              - "--task_run_id"
              - "{{task.run_id}}"

        - task_key: s01_preprocess
          depends_on:
            - task_key: s00_ingestion
          job_cluster_key: "cluster_dedicado"
          spark_python_task:
            python_file: "../src/steps/<nuevo_caso>/s01_preprocess.py"
            parameters:
              - "--config"
              - "${workspace.file_path}/config/config.yaml"
              - "--manifest"
              - "${workspace.file_path}/config/<nuevo_caso>_manifest.yaml"
              - "--run_id"
              - "{{job.run_id}}"
              - "--task_run_id"
              - "{{task.run_id}}"
```

### Paso 5: Actualizar `config.yaml`

Agrega solo la seccion que use tu caso:

```yaml
nuevo_caso:
  enabled: true
  batch_size: 50
  mode: "standard"
```

Si el caso usa LLM/embeddings/OCR/search, reusa secciones existentes:

- `llms`
- `embeddings`
- `ocr`
- `vector_stores`

### Paso 6: Actualizar `databricks.yml`

Incluye el nuevo resource:

```yaml
include:
  - resources/indexing_workflow.yml
  - resources/sharepoint_workflow.yml
  - resources/<nuevo_caso>_workflow.yml
```

En cada **target** (`dev`, etc.), bajo `resources.jobs`, añade un bloque para tu job con `job_clusters` (copia la estructura de `indexing_workflow_job` y reutiliza `cluster_dedicado` o define un `job_cluster_key` nuevo y repite el bloque `new_cluster:`). El `job_cluster_key` de cada task en el YAML del workflow debe coincidir con uno definido ahí.

Si aplica, ajusta también:

- `bundle.name`
- variables de cluster (`node_type_id`, `num_workers`, etc.)
- target (`workspace.host`, `workspace.root_path`)

### Paso 7: Validar y desplegar

```bash
databricks bundle validate
databricks bundle deploy
```

Luego ejecuta el job especifico en Databricks (indexacion, resumen o nuevo caso).

---

## 4) Patrones de adaptacion comunes

### Flujo lineal (secuencial)

Para procesos simples donde cada step depende del anterior:

```text
s00 -> s01 -> s02 -> s03
```

```yaml
# <nuevo_caso>_manifest.yaml (los steps van bajo workflow.steps)
workflow:
  catalog: "main"
  schema: "mi_schema"
  steps:
    s00_input:
      outputs: [{key: "data_raw", type: "table", medallion: "bronze"}]
    s01_process:
      inputs: [{from_step: "s00_input", key: "data_raw", type: "table"}]
      outputs: [{key: "data_processed", type: "table", medallion: "silver"}]
    s02_enrich:
      inputs: [{from_step: "s01_process", key: "data_processed", type: "table"}]
      outputs: [{key: "data_enriched", type: "table", medallion: "silver"}]
    s03_output:
      inputs: [{from_step: "s02_enrich", key: "data_enriched", type: "table"}]
      outputs: [{key: "data_final", type: "table", medallion: "gold"}]
```

### Flujo con ramas paralelas

Para procesar diferentes tipos de datos simultaneamente:

```text
s00 --+-- s01a --+
      +-- s01b --+-- s02 -> s03
      +-- s01c --+
```

Las tareas `s01a`, `s01b`, `s01c` corren en paralelo porque tienen el mismo padre y no dependen entre si.

### Flujo con merge

Para combinar resultados de multiples fuentes:

```yaml
workflow:
  catalog: "main"
  schema: "mi_schema"
  steps:
    s03_merge:
      inputs:
        - from_step: "s01_source_a"
          key: "data_a"
          type: "table"
        - from_step: "s02_source_b"
          key: "data_b"
          type: "table"
      outputs:
        - key: "merged_data"
          type: "table"
          medallion: "gold"
```

Ejemplo de merge en codigo (si usas Spark DataFrames):

```python
tbl_a = step_ctx.get_input("data_a")
tbl_b = step_ctx.get_input("data_b")
df_a = spark.table(tbl_a)
df_b = spark.table(tbl_b)
df_merged = df_a.unionByName(df_b, allowMissingColumns=True)
```

---

## 5) Checklist rapido antes de crear otro workflow

- [ ] Definiste DAG y dependencias
- [ ] Creaste `config/<nuevo_caso>_manifest.yaml`
- [ ] Creaste `src/steps/<nuevo_caso>/...`
- [ ] Creaste `resources/<nuevo_caso>_workflow.yml` (opcional: copiar desde `resources/new_workflow.yml`)
- [ ] Incluiste el resource en `databricks.yml` (`include:`)
- [ ] Definiste `job_clusters` para el nuevo job en el target de `databricks.yml`
- [ ] Ajustaste `config/config.yaml`
- [ ] `databricks bundle validate` sin errores

Con este patron, puedes reutilizar el template para multiples workflows asincronos manteniendo separacion clara entre indexacion, resumen, sharepoint y nuevos casos.

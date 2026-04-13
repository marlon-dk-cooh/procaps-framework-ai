# Guia de inicio

Esta guia explica como preparar el entorno, validar accesos, autenticarse en Databricks y realizar el primer despliegue del template `workflow`.

El objetivo es que puedas:

1. entender que necesitas antes de empezar,
2. validar la configuracion minima,
3. desplegar el bundle en `dev`,
4. ejecutar el workflow y revisar resultados.

---

## 1. Requisitos previos

### Herramientas

Antes de usar el template, asegúrate de tener:

- **Databricks CLI** instalado
- **Python 3.9+** disponible localmente
- acceso al repositorio y a la carpeta `workflow/`
- acceso al **workspace de Azure Databricks** donde harás el despliegue
- permisos para acceder a los servicios de Azure que consume el workflow

### Herramientas recomendadas

- `git`
- un editor como Cursor o VS Code
- terminal con acceso a `databricks`

---

## 2. Servicios de Azure requeridos

El archivo `workflow/config/config.yaml` ya define las integraciones funcionales que consume el template.

### Servicios actualmente configurados en `config.yaml`

| Servicio | Seccion | Uso principal |
|---|---|---|
| Azure Key Vault | `secrets` | Resolucion de secretos |
| Azure OpenAI | `llms`, `embeddings` | LLMs y embeddings |
| Azure AI Search | `vector_stores.vectordb_text` | Indice vectorial |
| Azure Document Intelligence | `ocr` | OCR de documentos |
| Azure Language / PII | `security.pii` | Deteccion de PII |
| Azure Content Safety | `security.content_safety` | Sanitizacion y seguridad |
| ADLS Gen2 | usado por `adls_uploader` | Lectura/escritura bronze, silver, gold |

### Lo que debes tener resuelto antes del primer despliegue

- el workspace de Databricks debe poder autenticarse contra Azure
- los secretos referenciados en `config.yaml` deben existir en Key Vault
- el storage account y sus filesystems (`bronze`, `silver`, `gold`) deben existir o ser accesibles
- el servicio de Azure AI Search debe estar disponible
- el endpoint/modelo de Azure OpenAI debe estar configurado

---

## 3. Configuracion minima a revisar

### `workflow/databricks.yml`

Debes revisar como minimo:

- `bundle.name`
- `targets.dev.workspace.host`
- `targets.dev.workspace.root_path`
- `targets.dev.variables.node_type_id`
- `targets.dev.variables.num_workers`
- `targets.dev.variables.spark_version`

### `workflow/config/config.yaml`

Debes confirmar:

- secretos y endpoints
- nombre del indice de Search
- dimensiones del embedding
- configuracion de OCR
- configuracion de PII / seguridad
- estrategia de chunking

### `workflow/config/indexing_manifest.yaml`

Debes revisar:

- `workflow.catalog`
- `workflow.schema`
- input de `s00_discovery`
- outputs logicos por step

## 4. Estructura del template

```text
workflow/
├── databricks.yml
├── requirements.txt
├── resources/
│   ├── indexing_workflow.yml
│   └── sharepoint_workflow.yml
├── config/
│   ├── config.yaml
│   ├── indexing_manifest.yaml
│   └── sharepoint_manifest.yaml
├── src/
│   ├── core/
│   └── steps/
│       ├── indexing/
│       └── sharepoint/
├── Pipeline/
└── docs/
```

### Que contiene cada parte

- `databricks.yml`: bundle, target `dev`, variables y cluster
- `resources/`: definicion de jobs
- `config/`: configuracion funcional y manifiestos
- `src/core/`: utilidades compartidas
- `src/steps/indexing/`: steps del workflow de indexacion
- `src/steps/sharepoint/`: step del job SharePoint -> Bronze standalone

---

## 5. Autenticacion con Databricks

### Ambiente `dev`

El bundle actual tiene definido el target:

- `dev`

Autenticacion sugerida:

```bash
databricks auth login ^
  --host https://<tu-workspace>.azuredatabricks.net ^
  --profile workflow-dev
```

Luego verifica:

```bash
databricks auth env --profile workflow-dev
```

### Otros ambientes

El `databricks.yml` actual solo define `dev` como target del bundle.

Si más adelante agregas `lab` o `prd`, deberás:

- crear esos targets en `databricks.yml`
- alinear los valores con el pipeline de CI/CD
- usar perfiles dedicados por ambiente

---

## 6. Primer despliegue

Ejecuta estos comandos desde la carpeta `workflow/`.

### 1. Validar el bundle

```bash
databricks bundle validate --profile workflow-dev
```

Esto valida:

- sintaxis del bundle,
- referencias a archivos,
- definicion de jobs,
- configuracion de targets,
- coherencia basica del despliegue.

### 2. Desplegar al ambiente dev

```bash
databricks bundle deploy --profile workflow-dev -t dev
```

Esto:

- sincroniza los archivos del proyecto al workspace de Databricks
- crea o actualiza el job con la configuracion del cluster
- configura las dependencias (librerias PyPI) en cada tarea

### 3. Verificar el despliegue

```bash
databricks bundle summary --profile workflow-dev -t dev
```

### 4. Ejecutar el workflow de indexacion

```bash
databricks bundle run --profile workflow-dev -t dev indexing_workflow_job
```

Esto inicia el job en Databricks. Se puede monitorear la ejecucion desde la UI de Databricks en la seccion **Workflows > Jobs**.

## 7. Preparar datos de entrada

Antes de ejecutar los jobs, debes tener archivos disponibles en el origen configurado.

### Workflow de indexacion

El step `s00_discovery` espera documentos en la ubicacion definida en:

- `workflow/config/indexing_manifest.yaml`

Actualmente el input logico es:

```yaml
inputs:
  - key: "raw_documents"
    type: "container"
    container_name: "bronze"
    path_prefix: "indexing"
```

Ajusta `path_prefix` al prefijo real bajo `bronze` donde esten (o llegaran) los documentos; debe coincidir con el destino del paso `s00_sharepoint` cuando sincronizas desde SharePoint antes del discovery.

### Recomendacion practica

Sube primero un conjunto pequeño de archivos de prueba:

- `.txt`
- `.md`
- `.json`
- `.csv`
- `.pdf`

asi puedes validar discovery, procesamiento y logs sin esperar corridas grandes.

---

## 8. Que revisar despues de ejecutar

### En Databricks UI

Revisa:

- el DAG del job
- el estado de cada task
- logs por task
- duracion total

### En storage / ADLS

Revisa artefactos en:

- `silver/source=...`
- `silver/logs/<run_id>/`
- `gold/logs/<run_id>/`

### Resultados esperados

#### Indexacion

- `discovery_manifest.json`
- `ir_data.json`
- `chunks_data.json`
- `sanitized_chunks.json`
- `embeddings_data.json`
- indice en Azure AI Search
- `run_summary.json`

---

## 9. Troubleshooting rapido

### `bundle validate` falla

Posibles causas:

- ruta `python_file` incorrecta
- YAML invalido
- variable faltante en `databricks.yml`

### El job falla al iniciar

Revisa:

- autenticacion del perfil Databricks
- `workspace.host`
- `root_path`
- disponibilidad del tipo de nodo del cluster

### Los steps fallan por acceso

Revisa:

- permisos sobre ADLS
- acceso a Key Vault
- secretos definidos correctamente
- endpoints y nombres de deployment en Azure OpenAI
- permisos sobre Azure AI Search

### No aparecen resultados

Verifica:

- que sí existan archivos en el origen configurado
- que las extensiones sean soportadas
- que los steps anteriores hayan escrito sus artefactos en `silver`

---

## 10. Siguiente paso recomendado

Una vez completes el primer despliegue:

1. prueba primero `indexing_workflow_job` con pocos archivos,
2. revisa logs y artefactos generados,
3. ajusta `config.yaml` y manifests para tu caso real,
4. luego automatiza el despliegue por pipeline.

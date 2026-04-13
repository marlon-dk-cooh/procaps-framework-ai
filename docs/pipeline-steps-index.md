# Steps del Pipeline de indexacion

Documentacion tecnica alineada con la implementacion actual de `src/steps/indexing`.

---

## Vista general del DAG

```text
s00_sharepoint (opcional: sync SharePoint -> Bronze si config)
          |
          v
    s00_discovery
  ├── s01_textual_ingestion
  ├── s01_visual_ingestion
  ├── s01_tabular_ingestion
  └── s01_structured_ingestion

s01_textual_ingestion + s01_tabular_ingestion + s01_structured_ingestion -> s02_chunking
s01_visual_ingestion -> s02_image_captioning
s02_chunking -> s03_sanitization
s03_sanitization + s02_image_captioning + s01_visual_ingestion -> s04_embeddings_keywords
s04_embeddings_keywords -> s05_indexing
s05_indexing -> s06_log_summary
```

Orquestacion en `resources/indexing_workflow.yml`: el task `s00_discovery` depende de `s00_sharepoint` (el sync real solo corre si `config.indexing.sharepoint_sync_enabled` es `true` y `sharepoint.enabled` no es `false`).

Carpeta de implementacion:

- `src/steps/indexing/`

---

## Convenciones de ejecucion

Todos los steps reciben por CLI:

- `--config`
- `--manifest`
- `--run_id`
- `--task_run_id`

Todos usan `StepContext` y registran observabilidad con `StepLogger`. Los logs por step se escriben en `silver/logs_indexing/<run_id>/<step_name>.json` (convencion definida en `src/core/medallion_paths.py`). El paso `s00_sharepoint` puede incluir el campo opcional `step_metadata` (contadores de sync, motivo si no corre, prefijo Bronze).

Los artefactos de datos del pipeline de indexacion bajo el filesystem **silver** se particionan con prefijo **`steps_indexing/`** (por ejemplo `silver/steps_indexing/source=<step>/year=.../...`). El codigo lista rutas con `indexing_source_list_prefix(...)` en cada step.

---

## s00_sharepoint - SharePoint a Bronze (antes del discovery)

- **Archivo**: `src/steps/indexing/s00_sharepoint.py`

### Proposito

Copiar archivos desde SharePoint Online al contenedor ADLS **bronze** usando el mismo prefijo de ruta que consumira `s00_discovery` (`indexing_manifest.yaml` → `s00_discovery` → `raw_documents` → `path_prefix`), para que el discovery encuentre los blobs sin otro ingesta manual.

### Activacion

| Condicion | Efecto |
|-----------|--------|
| `config.indexing.sharepoint_sync_enabled == true` | Permite ejecutar `SharePointToBronzeProcess` |
| `config.sharepoint.enabled != false` | Si es `false`, el paso no sincroniza (no-op con log) |

Si no se cumple, el step termina sin error y registra el motivo en `step_metadata.sync_disabled_reason` dentro del log en Silver.

### Entrada / contrato de manifiesto

| Fuente | Uso |
|--------|-----|
| `config.yaml` → `sharepoint.*` | Credenciales, `relative_path`, `skip_sync_if_unchanged`, etc. |
| `indexing_manifest.yaml` → `steps.s00_sharepoint.options` | `max_folders`, `only_folder`, `since_date` (opcionales; `null` o clave ausente = sin ese filtro). Job SharePoint standalone: mismas claves en `sharepoint_manifest.yaml` → `steps.s00_sharepoint_to_bronze.options`. |
| `indexing_manifest.yaml` → `steps.s00_discovery.inputs.raw_documents` | Se lee `path_prefix` para fijar el destino bajo `bronze` |

### Procesamiento

- inicializa `SharePointToBronzeProcess` (`src/core/sharepoint/sharepoint_connection.py`),
- fuerza `bronze_folder` al `path_prefix` de discovery cuando esta definido,
- sube blobs al filesystem **bronze**; contadores por corrida: documentos sincronizados, omitidos (sin cambios / filtro de fecha), fallidos.

### Salidas

| Artefacto | Capa |
|-----------|------|
| Blobs bajo `bronze/<path_prefix>/...` | bronze |
| `s00_sharepoint.json` (StepLogger + `step_metadata`) | silver (`logs_indexing/<run_id>/`) |

---

## s00_discovery - Descubrimiento, clasificacion y staging

- **Archivo**: `src/steps/indexing/s00_discovery.py`

### propósito

Escanear ADLS Bronze, clasificar archivos por extension, copiarlos a Silver staging y generar un manifiesto de discovery para los steps posteriores.

### Entrada

| Key | Tipo | Origen |
|---|---|---|
| `raw_documents` | container (manifest) | fuente inicial |

### Procesamiento

- escanea Bronze usando `path_prefix`,
- ignora prefijos `bronze/`, `silver/` y `gold/` si aparecen en el mismo contenedor,
- clasifica por extension:
  - `textual`: `.pdf`, `.docx`, `.txt`, `.pptx`
  - `tabular`: `.csv`, `.xlsx`, `.xls`
  - `images`: `.png`, `.jpg`, `.jpeg`, `.tiff`, `.webp`, `.gif`, `.bmp`
  - `structured`: `.json`, `.parquet`, `.delta`
- omite extensiones desconocidas,
- copia cada archivo a:
  - `silver/steps_indexing/source=s00_discovery/.../staging/<group>/<ext>/<filename>`

### Salidas

| Artefacto | Capa |
|---|---|
| `discovery_manifest.json` | silver |
| `discovery_report.txt` | silver |

Rutas principales:

- `silver/steps_indexing/source=s00_discovery/.../discovery/discovery_manifest.json`
- `silver/steps_indexing/source=s00_discovery/.../discovery/discovery_report.txt`

### Shape de `discovery_manifest.json`

```json
{
  "file_path": "indexing/docs/doc1.pdf",
  "staged_path": "steps_indexing/source=s00_discovery/year=2026/month=03/day=14/staging/textual/pdf/doc1.pdf",
  "filename": "doc1.pdf",
  "group": "textual",
  "extension": ".pdf",
  "size_bytes": 20480,
  "discovered_at": "2026-03-14T04:30:00"
}
```

---

## s01_textual_ingestion - Extraccion textual e IR

- **Archivo**: `src/steps/indexing/s01_textual.py`

### Proposito

Procesar archivos textuales y normalizarlos a `DocumentIR`.

### Entrada real

- lee el ultimo `discovery_manifest.json` bajo `steps_indexing/source=s00_discovery`,
- filtra `group == "textual"`,
- descarga el archivo con prioridad:
  1. `staged_path` en Silver
  2. `file_path` en Bronze como fallback

### Salida

| Artefacto | Capa |
|---|---|
| `ir_data.json` | silver |

Ruta:

- `silver/steps_indexing/source=s01_textual/.../ingestion/ir/ir_data.json`

### Shape de salida

Lista de objetos `DocumentIR` serializados:

- `doc_id`
- `source_path`
- `plain_text`
- `markdown_text`
- `structured_elements`
- `metadata`

Metadata agregada en el step:

- `original_filename`
- `source_path`
- `read_from`
- `ingestion_date`

### Servicios externos

- OCR (`ocr` config)
- `FileExtractor`
- `IRNormalizer`

---

## s01_visual_ingestion - Metadata de imagen y OCR best-effort

- **Archivo**: `src/steps/indexing/s01_visual.py`

### Proposito

Procesar imagenes, extraer metadata tecnica y enriquecer con OCR cuando es posible.

### Entrada real

- lee el ultimo `discovery_manifest.json` bajo `steps_indexing/source=s00_discovery`,
- filtra `group == "images"` con extensiones soportadas:
  - `.png`, `.jpg`, `.jpeg`, `.tiff`, `.bmp`, `.gif`, `.webp`
- descarga con prioridad `staged_path -> file_path`

### Salida

| Artefacto | Capa |
|---|---|
| `images_metadata.json` | silver |

Ruta:

- `silver/steps_indexing/source=s01_visual_ingestion/.../ingestion/images/images_metadata.json`

### Contenido de salida

- `file_id`
- `file_name`
- `source_path`
- `source_type = "image"`
- `type_source` por mime, por ejemplo `image/png`
- `image_size_bytes`
- `width_px`
- `height_px`
- `color_mode`
- `image_format`
- `metadata.ocr_text`
- `metadata.ocr_characters`
- `metadata.ocr_applied`
- `metadata.read_from`
- `metadata.ingestion_date`

### Servicios externos

- Pillow
- OCR (`ocr` config) de forma best-effort

---

## s01_tabular_ingestion - Ingesta tabular

- **Archivo**: `src/steps/indexing/s01_tabular.py`

### Proposito

Procesar archivos tabulares y convertir sus filas a registros indexables.

### Entrada real

- lee el ultimo `discovery_manifest.json` bajo `steps_indexing/source=s00_discovery`,
- filtra `group == "tabular"` con extensiones:
  - `.csv`, `.xlsx`, `.xls`
- descarga con prioridad `staged_path -> file_path`

### Reglas de procesamiento

- el archivo debe tener columna `content`,
- si no existe esa columna, el archivo se rechaza,
- cada fila con contenido no vacio se convierte en un registro,
- columnas adicionales se guardan en `metadata`,
- se agrega `read_from` en metadata.

### Salida

| Artefacto | Capa |
|---|---|
| `tabular_data.json` | silver |

Ruta:

- `silver/steps_indexing/source=s01_tabular_ingestion/.../ingestion/tabular/tabular_data.json`

### Shape de salida

- `file_id`
- `file_name`
- `file_type`
- `source_path`
- `content`
- `metadata`
- `run_id`

### Servicios externos

- Pandas
- openpyxl

---

## s01_structured_ingestion - Ingesta estructurada con inferencia

- **Archivo**: `src/steps/indexing/s01_structured.py`

### Proposito

Procesar datos estructurados y detectar campos utiles para RAG.

### Entrada real

- lee el ultimo `discovery_manifest.json` bajo `steps_indexing/source=s00_discovery`,
- filtra `group == "structured"` con extensiones:
  - `.json`, `.parquet`, `.delta`
- descarga con prioridad `staged_path -> file_path`

### Logica de procesamiento

Parseo por extension:

- `.json`: JSON documento o JSON lines
- `.parquet`: lectura con PyArrow
- `.delta`: en modo local no se procesa y devuelve sin registros

Resolucion de campos:

1. mapeos conocidos en `config.yaml -> json_file_structure.mappings`
2. inferencia con LLM si no hay mapeo conocido
3. fallback heuristico buscando columnas textuales

Reglas relevantes:

- para `.json`, se rechazan estructuras heterogeneas,
- si no se detectan campos utiles para RAG, el archivo se omite,
- filas con contenido menor a 50 caracteres se omiten.

### Salida

| Artefacto | Capa |
|---|---|
| `structured_data.json` | silver |

Ruta:

- `silver/steps_indexing/source=s01_structured_ingestion/.../ingestion/structured/structured_data.json`

### Shape de salida

- `file_id`
- `file_name`
- `file_type`
- `source_path`
- `content`
- `metadata`
- `run_id`

`metadata` incluye:

- campos metadata seleccionados,
- `_structure_analysis`,
- `read_from`

### Servicios externos

- Azure OpenAI para inferencia de campos
- PyArrow

---

## s02_chunking - Fragmentacion

- **Archivo**: `src/steps/indexing/s02_chunking.py`

### Proposito

Unificar documentos textuales, tabulares y estructurados en un formato comun y fragmentarlos en chunks.

### Entradas

| Artefacto | Origen |
|---|---|
| `ir_data.json` | `s01_textual_ingestion` |
| `tabular_data.json` | `s01_tabular_ingestion` |
| `structured_data.json` | `s01_structured_ingestion` |

### Procesamiento

- rehidrata `DocumentIR`,
- transforma tabular y structured al shape esperado por el chunker,
- si no hay entradas, termina sin salida,
- usa `processing.chunking.type_chunking` como estrategia base,
- puede aplicar override simplificado para Markdown,
- registra la estrategia efectiva usada.

### Salida

| Artefacto | Capa |
|---|---|
| `chunks_data.json` | silver |

Ruta:

- `silver/steps_indexing/source=s02_chunking/.../processing/chunks/chunks_data.json`

### Shape de salida

- `chunk_id`
- `content`
- `metadata.source_doc_id`
- `metadata.chunk_ordinal`
- `metadata.strategy`
- `metadata.original_metadata`

### Servicios externos

- modulo interno de chunking `src/core/chunking/*`

---

## s02_image_captioning - Captioning OCR-first

- **Archivo**: `src/steps/indexing/s02_image_captioning.py`

### Proposito

Convertir imagenes en texto indexable priorizando OCR y usando LLM vision solo cuando es necesario.

### Entrada

| Artefacto | Origen |
|---|---|
| `images_metadata.json` | `s01_visual_ingestion` |

### Estrategia

1. usar `metadata.ocr_text` si ya existe,
2. si no existe, intentar OCR sobre la imagen,
3. si no hay OCR util y captioning esta habilitado, generar caption con LLM vision,
4. si todo falla, usar fallback generico.

### Salida

| Artefacto | Capa |
|---|---|
| `image_captions.json` | silver |

Ruta:

- `silver/steps_indexing/source=s02_image_captioning/.../processing/image_captions/image_captions.json`

### Shape de salida

- `file_id`
- `file_name`
- `source_path`
- `source_type = "text"`
- `content`
- `metadata.origin`
- `metadata.mime_type`
- `metadata.ocr_applied`
- `run_id`

### Servicios externos

- OCR (`ocr` config)
- Azure OpenAI Vision para captions

---

## s03_sanitization - Sanitizacion de chunks

- **Archivo**: `src/steps/indexing/s03_sanitization.py`

### Proposito

Aplicar sanitizacion PII a los chunks antes de embeddings e indexacion.

### Entrada

| Artefacto | Origen |
|---|---|
| `chunks_data.json` | `s02_chunking` |

### Procesamiento

- si `security.pii.enabled == true`, aplica `PIIFilter`,
- si PII esta deshabilitado, hace passthrough del contenido,
- marca siempre:
  - `metadata.sanitized = true`
  - `metadata.source_step = "s03_sanitization"`

### Salida

| Artefacto | Capa |
|---|---|
| `sanitized_chunks.json` | silver |

Ruta:

- `silver/steps_indexing/source=s03_sanitization/.../processing/sanitized/sanitized_chunks.json`

### Servicios externos

- `PIIFilter`

---

## s04_embeddings_keywords - Embeddings y keywords

- **Archivo**: `src/steps/indexing/s04_embeddings_keywords.py`

### Proposito

Generar embeddings para chunks sanitizados e imagenes textualizadas, y opcionalmente generar keywords.

### Entradas

| Artefacto | Origen |
|---|---|
| `sanitized_chunks.json` | `s03_sanitization` |
| `image_captions.json` | `s02_image_captioning` |
| `images_metadata.json` | `s01_visual_ingestion` (declarado en `indexing_manifest.yaml` como input adicional) |

### Procesamiento

- transforma los captions de imagen al mismo shape que un chunk,
- genera embeddings por lotes con `EmbeddingsGenerator`,
- marca:
  - `metadata.embedded = true`
  - `metadata.source_step = "s04_embeddings_keywords"`
- keywords:
  - solo si `embeddings.keywords.enabled == true`,
  - usa LLM si esta configurado,
  - si el LLM falla, usa fallback heuristico,
  - si esta deshabilitado, deja lista vacia.

### Salida

| Artefacto | Capa |
|---|---|
| `embeddings_data.json` | silver |

Ruta:

- `silver/steps_indexing/source=s04_embeddings_keywords/.../processing/embeddings/embeddings_data.json`

### Shape de salida

- `chunk_id`
- `content`
- `vector`
- `metadata`

### Servicios externos

- Azure OpenAI embeddings
- Azure OpenAI para keywords si esta habilitado

---

## s05_indexing - Carga a Azure AI Search

- **Archivo**: `src/steps/indexing/s05_indexing.py`

### Proposito

Crear o validar el indice de Azure AI Search y hacer upsert de documentos vectoriales.

### Entrada

| Artefacto | Origen |
|---|---|
| `embeddings_data.json` | `s04_embeddings_keywords` |

### Procesamiento

- crea o valida el indice con `ensure_index_exists(...)`,
- construye un ID deterministico por documento,
- indexa solo items con `content` y `vector`,
- normaliza metadata para Search:
  - `source`: prioriza `original_filename`
  - `type_source`: normaliza mime conocidos a etiquetas cortas
- hace upsert,
- elimina documentos obsoletos ya no presentes en la corrida actual.

### Salida

| Artefacto | Tipo |
|---|---|
| `search_index` | indice externo |

### Shape de documento indexado

```json
{
  "id": "deterministic-id",
  "content": "texto del chunk",
  "content_vector": [0.01, 0.02, "..."],
  "metadata": {
    "count_characters": 1234,
    "source": "documento.pdf",
    "type_source": "pdf"
  },
  "keywords": ["k1", "k2"]
}
```

### Servicios externos

- Azure AI Search

---

## s06_log_summary - Consolidacion final de logs

- **Archivo**: `src/steps/indexing/s06_log_summary.py`

### Proposito

Consolidar logs del run y generar un resumen final en Gold.

### Entrada

| Fuente | Tipo |
|---|---|
| `silver/logs_indexing/<run_id>/*.json` | files json |

### Procesamiento

- agrega metricas de los steps (discovery, s01–s05, etc.),
- si existe el log `s00_sharepoint.json`, incorpora el bloque **`sharepoint_indexing`** (sync ejecutado o no, motivo, prefijo Bronze, contadores `documents_synced`, omitidos, fallidos) y **`metrics.sharepoint_documents_synced`**.

### Salida

| Artefacto | Capa |
|---|---|
| `run_summary.json` | gold |

Ruta:

- `gold/logs_indexing/<run_id>/run_summary.json`

---

## Resumen de destinos

| Step | Destino principal |
|---|---|
| `s00_sharepoint` | Bronze (blobs) + Silver (`logs_indexing/<run_id>/` StepLogger) |
| `s00` discovery a `s04` | Silver datos bajo `steps_indexing/...` + logs en `logs_indexing/<run_id>/` |
| `s05` | Azure AI Search |
| `s06` | Gold (`logs_indexing/<run_id>/run_summary.json`) |

---

## Resumen de servicios externos

| Step | ADLS | OCR | LLM | PII | Search | SharePoint |
|---|---|---|---|---|---|---|
| `s00_sharepoint` | Si | No | No | No | No | Si |
| `s00_discovery` | Si | No | No | No | No | No |
| `s01_textual_ingestion` | Si | Si | No | No | No | No |
| `s01_visual_ingestion` | Si | Si | No | No | No | No |
| `s01_tabular_ingestion` | Si | No | No | No | No | No |
| `s01_structured_ingestion` | Si | No | Si | No | No | No |
| `s02_chunking` | Si | No | No | No | No | No |
| `s02_image_captioning` | Si | Si | Si | No | No | No |
| `s03_sanitization` | Si | No | No | Si | No | No |
| `s04_embeddings_keywords` | Si | No | Si | No | No | No |
| `s05_indexing` | Si | No | No | No | Si | No |
| `s06_log_summary` | Si | No | No | No | No | No |

---

## Nota de consistencia

`indexing_manifest.yaml` define el contrato logico (`table`, `file`, `index`), mientras que la implementacion actual intercambia principalmente artefactos JSON en ADLS.

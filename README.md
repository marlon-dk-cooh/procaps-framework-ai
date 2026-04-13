## 📂 Workflow – Template de flujos asíncronos (Jobs de Databricks)

Esta carpeta contiene un **template de workflow asíncrono** basado en el bundle de indexación ubicado en `bundles/indexing/files`.  
La idea es que puedas **clonar este repositorio y reutilizar estos archivos en un nuevo repositorio o proyecto** para obtener, cambiando solo variables y rutas, un **pipeline de indexación multimodal para RAG** ejecutado como **Jobs de Databricks**.

Dentro de `workflow/` se modelan **múltiples flujos asíncronos**:

- **Flujo de indexación**: orquesta los steps `s00` a `s05` (`discovery`, `ingestion`, `chunking`, `sanitization`, `embeddings_keywords`, `indexing`) para construir y poblar los índices que usará el RAG.
- **Flujo de summary**: se apoya principalmente en el step `s06_log_summary` para generar resúmenes y métricas de la ejecución del workflow de indexación, también como Job asíncrono configurado en `resources/indexing_workflow.yml` y `config/indexing_manifest.yaml`.

---

## 🧱 Estructura del template

El template asume una estructura similar a:

```text
workflow/
├── databricks.yml           # Definición del Asset Bundle (jobs, clusters, variables)
├── src/
│   ├── steps/               # Steps del flujo (s00 a s06), alineados con bundles
│   ├── core/                # Utilidades del pipeline (settings, logging, storage, etc.)
│   └── libraries/           # Librerías auxiliares
├── resources/
│   └── indexing_workflow.yml  # Job de indexación (tasks s00_sharepoint–s06)
└── config/
    ├── config.yaml          # Configuración funcional del pipeline
    └── indexing_manifest.yaml  # DAG lógico de datos (inputs/outputs por step)
```

---

## 🚀 Cómo se ejecuta este workflow (igual que en `bundles`)

Antes de ejecutar el workflow, necesitas:

1. **Clonar el repositorio** (o el nuevo repo donde reutilices este template):

   ```bash
   git clone <URL_DEL_REPO>
   cd <CARPETA_DEL_REPO>
   cd workflow
   ```

2. **Instalar y configurar la Databricks CLI (v2)**:

   Sigue la guía oficial de instalación de la CLI según tu sistema operativo:  
   [`Install or update the Databricks CLI`](https://docs.databricks.com/aws/en/dev-tools/cli/install).

   - En Windows, por ejemplo, puedes usar `winget`:

     ```bash
     winget search databricks
     winget install Databricks.DatabricksCLI
     ```

   - Una vez instalada, configura la autenticación (token y URL del workspace) siguiendo la documentación de autenticación de la CLI.
   - El perfil/configuración que uses debe ser coherente con lo que referencias en `databricks.yml` (si aplicara).

Una vez hecho esto, este template sigue el mismo patrón de ejecución que el bundle de `bundles/indexing/files`:

3. **Validar el bundle**

   ```bash
   databricks bundle validate
   ```

4. **Desplegar el bundle (crear/actualizar el Job en Databricks)**

   ```bash
   databricks bundle deploy
   ```

5. **Ejecutar el workflow asíncrono (Job)**

   ```bash
   databricks bundle run
   ```

En un proyecto real, estos comandos se ejecutan **desde la carpeta donde tengas el `databricks.yml`** (por ejemplo, la raíz del repositorio que cree tu equipo usando este template).

---

## 🔧 ¿Dónde configurar las variables de los YAML?

En este template hay **tres YAML principales** y cada uno se toca para cosas distintas:

- **1. `databricks.yml`**  
  - Es el archivo principal del **Asset Bundle**.  
  - Aquí configuras:
    - `bundle.name`: nombre lógico del bundle.
    - `variables:` → parámetros generales (por ejemplo, `business_domain`, tamaños de cluster, etc.).
    - `targets.<entorno>.workspace.host` → URL del workspace de Databricks.
    - `targets.<entorno>.workspace.root_path` → ruta raíz donde se sincronizarán los archivos.
    - `targets.<entorno>.variables` → overrides por entorno (número de workers, tipo de nodo, etc.).
    - `targets.<entorno>.resources.jobs.indexing_workflow_job.job_clusters` → configuración del cluster efímero del job de indexación (y análogos para otros jobs del bundle).

- **2. `resources/indexing_workflow.yml`**  
  - Define el **Job asíncrono** de Databricks:
    - Nombre del job (`indexing_workflow_job`), descripción y tags.
    - Lista de **tasks** (`s00_sharepoint`, `s00_discovery`, `s01_*_ingestion`, `s02_chunking`, `s02_image_captioning`, `s03_sanitization`, `s04_embeddings_keywords`, `s05_indexing`, `s06_log_summary`).
    - Para cada task:
      - Script Python (`python_file`), por ejemplo `../src/steps/s00_discovery.py`.
      - Parámetros estándar: `--config`, `--manifest`, `--run_id`, `--task_run_id`.
      - Dependencias (`depends_on`) que hacen que el flujo sea **asíncrono** (tareas se ejecutan en paralelo cuando es posible).
      - Librerías `pypi` necesarias para ese step.
  - Normalmente solo modificas:
    - Rutas de los `python_file` si tus steps viven en otra carpeta.
    - Librerías si tu caso de uso requiere dependencias adicionales.

- **3. `config/indexing_manifest.yaml`**  
  - Define el **flujo lógico de datos** (DAG de inputs y outputs) que consumen los steps:
    - `catalog` y `schema`/`transversal_schema` donde se crean tablas y vistas.
    - Para cada step (`s00` a `s06`):
      - Inputs: tipo (`container`, `volume`, `table`, `index`, `file`), nombres de contenedores, rutas o claves de origen.
      - Outputs: tipo y `medallion` (`bronze`, `silver`, `gold`).
  - Aquí configuras:
    - En qué catálogo/esquema quieres guardar los resultados.
    - Dónde están los documentos de entrada (ruta de volumen o contenedor).
    - Cómo se llaman las tablas de salida (`ir_data`, `chunks_data`, `embeddings_data`, etc.).

En resumen:

- **Variables de entorno, cluster y workspace** → se configuran en `databricks.yml`.  
- **Tareas del Job (flujo asíncrono de steps)** → se definen en `resources/indexing_workflow.yml`.  
- **Camino de los datos (catálogos, tablas, volúmenes)** → se define en `config/indexing_manifest.yaml`.

---

## 🪄 Cómo usar este template en un proyecto nuevo

1. **Crea un nuevo repositorio y copia dentro**:
   - Este archivo `README.md`.
   - `databricks.yml` de esta carpeta.
   - `src/` completo de esta carpeta (incluye todos los steps `s00`–`s06` y el core común).
   - `resources/indexing_workflow.yml`.
   - `config/` completo de esta carpeta.

2. **Configura el entorno y los datos**:
   - En `databricks.yml`:
     - Ajusta `bundle.name` y `variables.business_domain` al dominio de negocio de tu proyecto.
     - Configura `targets.<entorno>.workspace.host` con la URL de tu workspace de Databricks.
     - Define `targets.<entorno>.workspace.root_path` donde se sincronizarán los archivos (por ejemplo, `/Repos/<tu_usuario>/<tu_repo>`).
     - Revisa el bloque de `job_clusters` para adaptar tamaño y tipo de nodos del cluster efímero.
   - En `config/indexing_manifest.yaml`:
     - Define el `catalog` y `schema` donde quieres que se creen tablas y vistas del pipeline.
     - Actualiza las rutas de entrada (volúmenes o contenedores) donde están tus documentos.
     - Revisa/ajusta los nombres de las tablas e índices de salida (por ejemplo, `ir_data`, `chunks_data`, `embeddings_data`, índices de búsqueda, etc.).
   - En `config/config.yaml`:
     - Configura endpoints externos (APIs, modelos, etc.).
     - Referencia los secretos necesarios (tokens, claves, etc.) usando el mecanismo de secretos de Databricks.
     - Ajusta parámetros funcionales del flujo (tamaños de chunk, filtros, flags de ejecución opcionales, etc.).

3. **Ejecuta el workflow en tu workspace**:

   Desde la carpeta donde tengas el `databricks.yml` de tu nuevo repo:

   ```bash
   databricks bundle validate   # Valida que el bundle y los YAML estén bien formados
   databricks bundle deploy     # Crea/actualiza el Job asíncrono en tu workspace
   databricks bundle run indexing_workflow_job   # Job de indexación (hay otros jobs en el bundle, p. ej. summary)
   ```

Con esto tendrás un **flujo asíncrono de Jobs de Databricks** (indexación y summary) inspirado en `bundles/indexing/files`, pero listo para reutilizar como template en otros proyectos o dominios de negocio.


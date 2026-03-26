# 🧬 ProCaps Framework AI

> **Pipeline inteligente de procesamiento de documentos para ProCaps** — impulsado por Azure AI Document Intelligence, Azure OpenAI y Databricks.

Este proyecto implementa un **pipeline de orquestación basado en DAG** para ingestar, extraer y transformar documentos no estructurados (PDFs, imágenes, DOCX, XLSX) en datos estructurados y consultables, utilizando Azure AI Document Intelligence y Azure Data Lake Storage Gen2.

---

## 📐 Arquitectura

El framework sigue un enfoque de **pipeline DAG modular**, donde cada paso (`step`) es un notebook de Databricks que se encadena secuencialmente. La arquitectura no emplea puertos ni adaptadores hexagonales; en su lugar, prioriza una separación limpia entre **modelos de datos (core)**, **clientes de infraestructura** y **pasos del pipeline**.

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DATABRICKS JOB (DAG)                          │
│        s00 (Read) ──▶ s01 (OCR/Extract) ──▶ s02 (Futuro)            │
├─────────────────────────────────────────────────────────────────────┤
│         CAPA CORE (Modelos de datos / Dataclasses)                   │
├─────────────────────────────────────────────────────────────────────┤
│         CAPA DE INFRAESTRUCTURA (Clientes de Servicios Azure)       │
│  - StorageAccount (ADLS Gen2)        - DocumentIntelligenceConnection│
├─────────────────────────────────────────────────────────────────────┤
│         UTILIDADES (Logger, LLM Client)                              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Estructura del Proyecto

```
procaps-framework-ai/
├── config/
│   ├── __init__.py
│   └── settings.py                    # Configuración centralizada (pydantic-settings + .env)
├── src/
│   ├── core/
│   │   └── models.py                  # Modelos de datos puros (ej. AnalyzedDocument)
│   ├── infrastructure/
│   │   └── connections.py             # Clientes Azure: StorageAccount, DocumentIntelligenceConnection
│   ├── steps/
│   │   ├── s00_read_files.py          # Paso 0: Listado y clasificación de archivos desde ADLS
│   │   ├── s01_extract_ocr.py         # Paso 1: Extracción OCR con Document Intelligence
│   │   └── s02_non_structured_data_processing.py  # Paso 2: Procesamiento de datos no estructurados (futuro)
│   └── utils/
│       ├── app_logger.py              # Logger centralizado del proyecto
│       └── llm_client.py              # Cliente LLM (Azure OpenAI vía LangChain)
├── main.py                            # Punto de entrada local para orquestar los pasos del pipeline
├── config.json                        # Mapa de configuración para AI Search y CosmosDB
├── databricks.yml                     # Definición del DAG y asset bundle de Databricks
├── pyproject.toml                     # Metadatos del proyecto y dependencias (uv)
├── uv.lock                            # Archivo de bloqueo de dependencias
├── .env                               # Variables de entorno (no se sube al repositorio)
├── .gitignore
└── README.md
```

---

## 🗺️ Hoja de Ruta Inicial

| #  | Tarea                         | Componente             | Capa               | Descripción                                                                 |
|----|-------------------------------|------------------------|---------------------|-----------------------------------------------------------------------------|
| 1  | **Leer Archivos de Entrada**  | ADLS Gen2              | Ingesta             | Detecta archivos `.pdf`, `.docx`, `.png` en el contenedor **Bronze**.       |
| 2  | **No Estructurado / OCR**     | Doc Intelligence       | Transformación      | Convierte imágenes/PDFs estilo ProCaps en texto estructurado vía Azure AI.  |
| 3  | **Conexiones (DI / OAI)**     | Adaptadores API        | Infraestructura     | Puente seguro (Managed Identity) hacia los servicios de Azure AI.           |
| 4  | **Archivos de Configuración** | Configuración del Job  | Orquestación        | Mapa de configuración para AI Search y CosmosDB.                            |

---

## ⚙️ Prerrequisitos

- **Python** >= 3.12
- **[uv](https://docs.astral.sh/uv/)** — Gestor de paquetes rápido para Python
- **Suscripción de Azure** con los siguientes servicios aprovisionados:
  - Azure AI Document Intelligence
  - Azure OpenAI
  - Azure Data Lake Storage Gen2 (ADLS)
- **Workspace de Databricks** (para la orquestación del DAG)

---

## 🚀 Primeros Pasos

### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd procaps-framework-ai
```

### 2. Instalar dependencias

```bash
uv sync
```

Esto creará un entorno virtual `.venv` e instalará todas las dependencias definidas en `pyproject.toml`.

### 3. Configurar variables de entorno

Crear un archivo `.env` en la raíz del proyecto (ya está incluido en `.gitignore`):

```env
# Azure Document Intelligence
AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT=https://<tu-recurso>.cognitiveservices.azure.com/
AZURE_DOCUMENT_INTELLIGENCE_API_KEY=<tu-api-key>

# Azure OpenAI (futuro)
AZURE_OPENAI_ENDPOINT=https://<tu-recurso>.openai.azure.com/
AZURE_OPENAI_API_KEY=<tu-api-key>

# ADLS Gen2 (futuro)
ADLS_ACCOUNT_NAME=<tu-cuenta-de-almacenamiento>
ADLS_CONTAINER_NAME=bronze
```

### 4. Ejecutar un paso localmente

```bash
uv run python -m src.steps.s00_read_files
```

---

## 🔐 Seguridad

- **Nunca subir** archivos `.env` ni claves API al control de versiones.
- En producción, la autenticación debe usar **Azure Managed Identity** en lugar de claves API.
- Todos los secretos en Databricks deben gestionarse mediante **Databricks Secrets** o **Azure Key Vault**.

---

## 🛠️ Stack Tecnológico

| Tecnología                     | Propósito                                    |
|--------------------------------|----------------------------------------------|
| Python 3.12+                   | Lenguaje principal                           |
| Azure AI Document Intelligence | OCR y extracción de documentos               |
| Azure OpenAI                   | Embeddings e IA generativa (futuro)          |
| ADLS Gen2                      | Almacenamiento de archivos crudos (capa Bronze) |
| Databricks                     | Orquestación del DAG y Unity Catalog         |
| CosmosDB                       | Persistencia de datos estructurados (futuro) |
| Azure AI Search                | Índice de búsqueda sobre datos extraídos (futuro) |
| uv                             | Gestión rápida de dependencias               |

---

## 📄 Licencia

*Por definir — Uso interno de ProCaps.*

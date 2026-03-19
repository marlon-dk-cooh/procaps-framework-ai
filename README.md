# 🧬 ProCaps Framework AI

> **Pipeline inteligente de procesamiento de documentos para ProCaps** — impulsado por Azure AI Document Intelligence, Azure OpenAI y Databricks.

Este proyecto implementa un **pipeline de orquestación basado en DAG** con una **arquitectura Hexagonal (Puertos y Adaptadores)** para ingestar, extraer y transformar documentos no estructurados (PDFs, imágenes, DOCX) en datos estructurados y consultables.

---

## 📐 Arquitectura

El framework sigue un enfoque híbrido de **DAG + Arquitectura Hexagonal**:

- **DAG (Grafo Acíclico Dirigido)**: Define el flujo de trabajo paso a paso, orquestado como un Job de Databricks.
- **Hexagonal (Puertos y Adaptadores)**: Mantiene la lógica de negocio desacoplada de los servicios específicos de la nube, permitiendo pruebas agnósticas al proveedor y una separación limpia de responsabilidades.

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DATABRICKS JOB (DAG)                          │
│          s00 (Read) ──▶ s01 (OCR/Extract) ──▶ s02 (Futuro)          │
├─────────────────────────────────────────────────────────────────────┤
│         CAPA DE APLICACIÓN (Interfaces / Lógica de Negocio)         │
├─────────────────────────────────────────────────────────────────────┤
│         CAPA DE INFRAESTRUCTURA (Adaptadores / Servicios Azure)     │
│  - Unity Catalog (Volumes/Tables)    - Azure Doc Intelligence       │
│  - Azure OpenAI (Embeddings)         - ADLS Gen2 (Bronze)           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Estructura del Proyecto

```
procaps-framework-ai/
├── src/
│   ├── core/                          # Dominio / Lógica de negocio (puertos, interfaces)
│   ├── infrastructure/
│   │   ├── adapters/                  # Adaptadores de servicios Azure (DI, OpenAI, ADLS)
│   │   ├── connections.py             # Conexiones de clientes API (Managed Identity)
│   │   └── settings/
│   │       └── settings.py            # Configuración del job y variables de entorno
│   └── steps/
│       ├── connections.py             # Helpers de conexión a nivel de step
│       ├── s00_read_files.py          # Paso 0: Lectura de archivos desde el contenedor Bronze
│       └── s01_extract_ocr.py         # Paso 1: Extracción OCR con Document Intelligence
├── config.json                        # Mapa de configuración para AI Search y CosmosDB
├── databricks.yml                     # Configuración del asset bundle de Databricks
├── pyproject.toml                     # Metadatos del proyecto y dependencias (uv)
├── uv.lock                           # Archivo de bloqueo de dependencias
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

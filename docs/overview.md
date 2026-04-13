# Descripcion general

## Que es este template

`workflow/` es un template reutilizable para construir **flujos asincronos de IA** ejecutados como **Databricks Jobs** y desplegados mediante **Databricks Asset Bundles**.

Su objetivo es ofrecer una base comun para casos de uso donde el procesamiento:

- no ocurre en tiempo real,
- requiere varios pasos con dependencias,
- necesita escalar con infraestructura efimera,
- debe quedar versionado, observable y desplegable por ambiente.

En lugar de empezar cada proyecto desde cero, este template ya entrega la estructura base de:

- configuracion,
- orquestacion,
- steps,
- despliegue,
- documentacion.

---

## Que problema resuelve

En proyectos de IA empresarial es comun necesitar pipelines que:

- procesen documentos, archivos o datos en segundo plano,
- combinen OCR, LLMs, embeddings y almacenamiento,
- ejecuten pasos en secuencia o en paralelo,
- registren logs, metricas y artefactos por corrida,
- se desplieguen de forma consistente entre ambientes.

Este template resuelve ese problema con un patron reutilizable sobre Databricks.

---

## Que incluye hoy este template

Actualmente el template incluye como referencia principal el **workflow de indexacion** (RAG) y, de forma independiente, el **job SharePoint → Bronze** (`sharepoint_workflow_job`).

### Workflow de indexacion

Pipeline asincrono orientado a construir un indice para escenarios de RAG.

Flujo general:

1. descubrir documentos,
2. ingestar contenido,
3. fragmentar en chunks,
4. sanitizar,
5. generar embeddings,
6. indexar en Azure AI Search,
7. consolidar logs del run.

### Job SharePoint → Bronze (opcional)

Sincroniza contenido desde SharePoint Online hacia ADLS (contenedor `bronze`) con su propio manifiesto y DAG; puede usarse solo o como paso previo al discovery de indexacion cuando esta habilitado el sync en el job de indexacion.

Estos casos demuestran el patron, pero la estructura esta pensada para extenderse a muchos otros flujos asincronos.

---

## Casos de uso que se pueden construir

La misma base se puede adaptar para:

- indexacion para RAG,
- generacion de resumenes,
- clasificacion de PQRs

La idea central es siempre la misma:

**un DAG de tasks en Databricks + configuracion externa + steps desacoplados por responsabilidad**.

---

## Estructura general del template

```text
workflow/
├── databricks.yml
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
│       ├── sharepoint/
│       └── new_workflow/
├── Pipeline/
└── docs/
```

### Rol de cada carpeta

- `databricks.yml`: define el bundle, targets y cluster.
- `resources/`: define los jobs y sus tasks.
- `config/`: define configuracion funcional y contratos logicos del DAG.
- `src/core/`: utilidades compartidas del framework.
- `src/steps/`: implementacion de la logica de cada workflow.
- `Pipeline/`: automatizacion CI/CD.
- `docs/`: documentacion tecnica y operativa.

---

## Como se organiza un workflow dentro del template

Cada workflow tiene tres piezas principales:

### 1. Definicion del job

Archivo en `resources/*.yml` con:

- tasks,
- dependencias,
- script Python de cada step,
- librerias,
- timeouts,
- retries.

### 2. Manifest del workflow

Archivo en `config/*_manifest.yaml` con:

- steps del flujo,
- inputs y outputs,
- tipo de artefacto,
- medallion o destino logico,
- catalog/schema cuando aplica.

### 3. Implementacion de steps

Scripts Python en `src/steps/<workflow>/` con la logica de negocio de cada paso.

---

## Tecnologias principales

| Componente | Tecnologia |
|---|---|
| Orquestacion | Databricks Jobs |
| Despliegue | Databricks Asset Bundles |
| Compute | Azure Databricks |
| Almacenamiento | ADLS Gen2 / medallion bronze-silver-gold |
| Procesamiento | Python |
| OCR | Azure Document Intelligence |
| LLM / Embeddings | Azure OpenAI |
| Busqueda vectorial | Azure AI Search |
| Secretos | Azure Key Vault |
| CI/CD | Azure DevOps Pipeline |

---

## Como se usa en un proyecto nuevo

La forma recomendada de usar este template es:

1. copiar la carpeta `workflow/` al nuevo repositorio o punto de partida,
2. ajustar `databricks.yml`,
3. ajustar `config/config.yaml`,
4. ajustar el manifest del workflow que se va a usar,
5. adaptar o crear steps en `src/steps/`,
6. validar y desplegar el bundle.

Si el caso de uso es nuevo, se puede crear:

- un nuevo `resources/<caso>_workflow.yml`,
- un nuevo `config/<caso>_manifest.yaml`,
- una nueva carpeta `src/steps/<caso>/`.

---

## Relacion con el resto del framework

Este template cubre la parte de **procesamiento asincrono**.

En una arquitectura mayor, normalmente se complementa con:

- agentes conversacionales,
- aplicaciones de consulta,
- APIs,
- tableros de monitoreo,
- componentes de ingesta o activacion por eventos.

En otras palabras:

- `workflow/` procesa y prepara datos,
- otros componentes consumen esos resultados.

---

## En que orden leer la documentacion

Para entender el template rapidamente:

1. `docs/overview.md`
2. `docs/architecture.md`
3. `docs/configuration.md`
4. `docs/customization.md`
5. `docs/pipeline-steps-index.md`
6. `docs/deployment.md`

---

## Resumen

Este template no representa un unico pipeline fijo.
Representa un **patron reusable para construir workflows asincronos de IA en Databricks**.

La indexacion incluida sirve como ejemplo funcional de ese patron y como base para extenderlo a nuevos casos de uso sin rehacer la arquitectura desde cero.
# Descripcion General

## Que es el Workflow Template

El Workflow Template es un **template reutilizable** del Framework de IA v2 que permite crear flujos de procesamiento asincronos desplegados como **Databricks Jobs** mediante **Asset Bundles**.

A diferencia de aplicaciones interactivas, estos workflows se ejecutan **por demanda o mediante triggers** (schedulers, eventos, APIs) y procesan datos de forma autonoma sin intervencion del usuario en tiempo real.

## Problema que resuelve

En proyectos de IA empresarial se necesitan flujos de procesamiento que:

- **Procesen grandes volumenes de datos** de forma asincrona (documentos, imagenes, datos estructurados)
- **Orquesten multiples pasos** con dependencias entre ellos (DAGs)
- **Escalen horizontalmente** usando infraestructura efimera (clusters bajo demanda)
- **Sean reproducibles y auditables** con tracking de metricas, alertas y artefactos
- **Se desplieguen de forma controlada** entre ambientes (dev → prod) con infraestructura como codigo

Este template provee toda esa infraestructura ya resuelta, para que los equipos solo se enfoquen en la logica de negocio de su caso de uso.

## Caso de ejemplo: Indexacion para RAG

El template viene con un caso completo de ejemplo: un pipeline de **indexacion multimodal para RAG** que:

1. **Descubre y clasifica** archivos de un volumen de entrada (PDFs, imagenes, CSVs, JSONs, etc.)
2. **Ingesta** cada tipo de archivo con el procesador adecuado (OCR, parsers, LLM)
3. **Fragmenta** el contenido en chunks optimizados para busqueda semantica
4. **Sanitiza** el contenido (deteccion de PII, seguridad de contenido)
5. **Genera embeddings** vectoriales con Azure OpenAI
6. **Indexa** en Azure Cognitive Search para busqueda vectorial

El resultado es un **indice vectorial listo para ser consumido** por un agente conversacional (implementado en el Agent Template del framework).

## Otros casos de uso posibles

El template no se limita a indexacion RAG. La misma estructura se puede adaptar para:

| Caso de Uso | Descripcion |
|-------------|-------------|
| **Clasificacion de PQRs** | Recibir peticiones/quejas/reclamos, clasificarlas automaticamente con LLM, enrutar y almacenar resultados |
| **Verificacion de documentos** | Recibir documentos de identidad o contratos, validar autenticidad y completitud via OCR + reglas |
| **Enriquecimiento de datos** | Tomar registros crudos, enriquecerlos con fuentes externas o LLM, y persistir datos mejorados |
| **ETL inteligente** | Procesar datos no estructurados, extraer entidades, normalizar y cargar en tablas analiticas |
| **Generacion de reportes** | Compilar datos de multiples fuentes, generar resumen con LLM, exportar reportes |

En todos los casos el patron es el mismo: **un DAG de steps que se ejecuta de forma asincrona sobre Databricks**.

## Posicion en el Framework

```
                    ┌──────────────────────────────┐
                    │     Framework de IA v2        │
                    └──────────────┬───────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                     │
    ┌─────────▼──────────┐  ┌─────▼──────────┐  ┌──────▼───────┐
    │  Workflow Template  │  │ Agent Template │  │    Otros     │
    │  (este repo)        │  │ (otro repo)    │  │  templates   │
    └─────────┬──────────┘  └─────┬──────────┘  └──────────────┘
              │                    │
    Flujos asincronos     Agentes conversacionales
    - Indexacion RAG      - Chat con contexto
    - Clasificacion       - Busqueda semantica
    - Verificacion        - Respuesta aumentada
    - ETL inteligente
```

## Tecnologias principales

| Componente | Tecnologia |
|------------|------------|
| Orquestacion | Databricks Jobs (Asset Bundles) |
| Compute | Clusters efimeros en Azure Databricks |
| Almacenamiento | Unity Catalog (tablas y volumenes) |
| Procesamiento | PySpark + Pandas |
| Servicios de IA | Azure OpenAI, Document Intelligence, Content Safety, Language Services |
| Busqueda vectorial | Azure Cognitive Search |
| Secretos | Azure Key Vault |
| IaC | Databricks Asset Bundles (Terraform internamente) |
| CI/CD | Pipeline automatizado con Service Principal (validacion de deps, APIs, cobertura) |

## Modelo de despliegue

El template sigue un flujo de ramas con CI/CD automatizado:

```
feature/* ──→ Integration ──→ master
   │              │               │
   ▼              ▼               ▼
  Dev          CI/CD           CI/CD
(manual)    (automatico)    (automatico)
```

- **Dev**: El desarrollador despliega manualmente con su perfil de Databricks, ejecuta y revisa en su workspace personal
- **Integration**: Al hacer merge a `Integration`, el pipeline de CI/CD valida dependencias, APIs hardcodeadas y cobertura. El Service Principal despliega automaticamente
- **Produccion**: Al hacer merge a `master`, el mismo pipeline CI/CD despliega a produccion via Service Principal

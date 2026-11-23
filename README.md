# Neurological Data Lake
## Documentación general del proyecto

### 1. Descripción

El proyecto implementa un Data Lake reproducible para integrar información de salud, demográfica y geográfica.

La arquitectura utiliza cuatro capas: **Landing, Bronze, Silver y Gold**. A partir de una misma fuente de datos se construyen tres escenarios independientes: Perfect, Medium y Poor.

### 2. Fuentes de datos

| Fuente | Descripción |
|---|---|
| ACS 2024 | American Community Survey, estimaciones demográficas DP05 a cinco años. |
| ACS County 2024 | Estimaciones demográficas DP05 a nivel de condado. |
| BRFSS 2023 | Behavioral Risk Factor Surveillance System. |
| PLACES 2025 | Local Data for Better Health, publicación 2025 con datos correspondientes a 2023. |

### 3. Arquitectura

| Capa | Responsabilidad |
|---|---|
| Landing | Recepción y organización de los archivos originales. |
| Bronze | Archivos Parquet y metadatos de ingesta simulada. |
| Silver | Datos tipificados, normalizados y estructurados. |
| Gold | Modelo dimensional e integración analítica. |

#### 3.1. Escenarios de calidad

| Escenario | Base de datos | Frecuencia |
|---|---|---|
| Perfect | `health_datalake` | Daily |
| Medium | `health_datalake_medium` | Every 3 days |
| Poor | `health_datalake_poor` | Every 6 days |

Medium y Poor se generan a partir del escenario original, mediante procesos independientes de degradación controlada.

#### 3.2. Esquemas PostgreSQL

| Esquema | Responsabilidad | Tablas |
|---|---|---:|
| `silver` | Datos estructurados y normalizados. | 9 |
| `gold` | Modelo dimensional y productos analíticos. | 17 |
| `datalake_control` | Ejecuciones ETL, lotes y metadatos. | 3 |
| **Total** | | **29** |

### 4. Tecnologías

- Python 3.12.
- PostgreSQL 17.
- Docker y Docker Compose v2.
- Pandas, PyReadStat y PyArrow.
- SQLAlchemy y Psycopg.
- JSONB, PyYAML y python-dotenv.

### 5. Requisitos

- Git.
- Python 3.12.x.
- Docker Engine o Docker Desktop.
- Docker Compose v2.

### 6. Instalación

Clonar el repositorio:

```bash
git clone -b develop https://github.com/antonia-cordova/neurological-data-lake.git
cd neurological-data-lake
```

Crear y activar el entorno virtual en Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Instalar dependencias:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements-lock.txt
python -m pip check
```

Configurar las variables de entorno:

```bash
cp .env.example .env
```

Editar `.env` con las credenciales de PostgreSQL correspondientes a la configuración de Docker Compose.

Los archivos originales deben descargarse desde Google Drive y ubicarse en las rutas esperadas por el proyecto. No se incluyen en GitHub.

### 7. Generación de Bronze

El módulo `scripts/bronze/bronze_simulator.py` genera archivos Parquet para los cuatro datasets.

El calendario simulado comprende desde el 1 de enero de 2023 hasta el 31 de diciembre de 2024.

| Frecuencia | Fechas |
|---|---:|
| Daily | 731 |
| Every 3 days | 244 |
| Every 6 days | 122 |

La distribución de registros es determinista y conserva los valores originales.

Cada registro incorpora siete metadatos de ingesta: identificador de origen, archivo fuente, fecha de recepción, secuencia, lote, escenario y hash de la fila original.

### 8. Ejecución integral

El proceso se coordina mediante un único punto de entrada:

```bash
python -m scripts.run_pipeline
```

La ejecución predeterminada procesa Perfect, Medium y Poor, en ese orden.

Para validar previamente las condiciones de ejecución:

```bash
python -m scripts.run_pipeline --preflight-only
```

### 9. Capa Silver

Silver contiene 9 tablas destinadas a organizar los datos sanitarios, demográficos y geográficos.

Sus procesos incluyen tipificación, normalización, validación, integración geográfica y trazabilidad de ejecuciones ETL.

El presupuesto de almacenamiento y los bloqueos transaccionales se gestionan mediante `scripts/silver/silver_budget.py` y PostgreSQL.

### 10. Modelo dimensional Gold

Gold contiene 17 tablas organizadas en dimensiones, hechos y correspondencias.

Incluye información de:

- Fuentes y geografías.
- Indicadores ACS.
- Variables BRFSS.
- Datos PLACES.
- Indicadores ACS nacionales y por condado.
- Integración sanitaria y geográfica.

El modelo permite relacionar información sanitaria y demográfica mediante claves geográficas, indicadores y referencias de ejecución.

### 11. Quality Generator

El módulo `scripts/quality/quality_generator.py` aplica degradación controlada sobre Medium y Poor.

| Dimensión | Medium | Poor |
|---|---:|---:|
| Accuracy | 5 % | 25 % |
| Completeness | 5 % | 25 % |
| Semantic Consistency | 5 % | 25 % |

La selección de cambios es determinista y utiliza Perfect como universo de referencia.

El proceso incorpora controles transaccionales, validación de restricciones, registro de cambios y recuperación de Ground Truth.

La validación independiente se ejecuta con:

```bash
python -m scripts.quality.validate_ground_truth --level all
```

Los informes se almacenan en:

```text
quality/reports/
quality/ground_truth/
```

### 12. Trazabilidad y validaciones

El esquema `datalake_control` contiene:

| Tabla | Responsabilidad |
|---|---|
| `etl_runs` | Registro de ejecuciones ETL y resultados. |
| `ingestion_batches` | Trazabilidad de los lotes Bronze. |
| `scenario_metadata` | Configuración y estado de cada escenario. |

Los procesos incorporan validaciones de fuentes, manifiestos, conteos, integridad referencial, restricciones y relaciones entre capas.

### 13. Estructura general

```text
neurological-data-lake/
│
├── data/
│   ├── landing/
│   └── bronze/
│
├── docs/
│
├── quality/
│   ├── reports/
│   └── ground_truth/
│
├── scripts/
│   ├── bronze/
│   ├── landing/
│   ├── silver/
│   ├── gold/
│   ├── quality/
│   ├── database_config.py
│   ├── database_connection.py
│   ├── initialize_database.py
│   ├── run_pipeline.py
│   ├── run_silver_pipeline.py
│   └── run_gold_pipeline.py
│
├── sql/
│   ├── silver/
│   └── gold/
│
├── .env.example
├── docker-compose.yml
├── requirements.txt
└── requirements-lock.txt
```

Los archivos de origen y los Parquet generados se mantienen fuera del control de versiones.

### 14. Estado del proyecto

El proyecto cuenta con las capas Landing, Bronze, Silver y Gold, tres escenarios independientes y mecanismos de degradación, recuperación, validación y trazabilidad.
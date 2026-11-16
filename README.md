# Neurological Data Lake

El presente proyecto implementa una arquitectura de tres capas: **Bronze, Silver y Gold**. A partir de una misma fuente de datos se construyen tres versiones independientes: una versión original (perfect) y dos versiones con distintos niveles de degradación controlada (medium y poor).

---

## 1. Fuentes de datos

| Fuente | Descripción |
|---|---|
| ACS 2024 | American Community Survey, estimaciones demográficas DP05 a cinco años. |
| ACS County 2024 | Estimaciones demográficas DP05 a nivel de condado. |
| BRFSS 2023 | Behavioral Risk Factor Surveillance System. |
| PLACES 2025 | Local Data for Better Health, publicación 2025, con observaciones correspondientes a 2023. |

---

## 2. Capas

| Capa | Descripción |
|---|---|
| Landing | Recepción y organización de los archivos de origen. |
| Bronze | Generación y almacenamiento de archivos Parquet con metadatos de ingesta simulada. |
| Silver | Datos transformados, estructurados, tipados y preparados para análisis. |
| Gold | Modelo dimensional, tablas de hechos, dimensiones, integración geográfica y consolidación analítica. |

### 2.1. Versiones de calidad

El proyecto utiliza tres bases de datos PostgreSQL independientes:

| Escenario | Base de datos | Descripción |
|---|---|---|
| Perfect | `health_datalake` | Escenario de referencia, sin degradación intencional. |
| Medium | `health_datalake_medium` | Escenario con degradación controlada. |
| Poor | `health_datalake_poor` | Escenario con mayor degradación controlada. |

Medium y Poor se generan independientemente a partir del escenario original.


### 2.2. Organización de esquemas en PostgreSQL

Cada base de datos organiza sus tablas en tres esquemas:

| Esquema | Responsabilidad | Tablas |
|---|---|---:|
| `silver` | Datos depurados, normalizados y estructurados. | 9 |
| `gold` | Modelo dimensional y productos analíticos. | 17 |
| `datalake_control` | Control de ejecuciones, lotes de ingesta y metadatos del escenario. | 3 |
| **Total** | | **29** |

---

## 3. Tecnologías

- Python 3.12.
- PostgreSQL 17.
- Docker y Docker Compose v2.
- Pandas.
- PyReadStat.
- PyArrow.
- SQLAlchemy.
- Psycopg.
- JSONB.
- PyYAML.
- python-dotenv.

---

## 4. Requisitos

- Git.
- Python 3.12.x.
- Docker Desktop o Docker Engine.
- Docker Compose v2.

---

## 5. Instalación y configuración

### 5.1. Clonar el repositorio

```bash
git clone -b develop https://github.com/antonia-cordova/neurological-data-lake.git

cd neurological-data-lake
```

### 5.2. Incorporar los datos de origen

Los archivos de datos originales se encuentran en una carpeta en Google Drive y no se incluyen directamente en GitHub.

Se deben descargar, extraer y ubicar en la estructura esperada por el proyecto, respetando los nombres y las rutas configuradas para cada dataset.

La capa Bronze se genera a partir de los datos de entrada mediante el simulador de ingesta.

### 5.3. Crear el entorno virtual

```bash
python3 -m venv .venv

source .venv/bin/activate
```

### 5.4. Instalar dependencias

```bash
python -m pip install --upgrade pip

python -m pip install -r requirements-lock.txt

python -m pip check
```

### 5.5. Configurar variables de entorno

Crear el archivo `.env` a partir del archivo de ejemplo:

```bash
cp .env.example .env
```

Editar el archivo `.env` y configurar las credenciales de PostgreSQL.

La configuración debe coincidir con la utilizada por Docker Compose y los procesos Python.

---

## 6. Ingesta y generación de la capa Bronze

La capa Bronze se genera mediante `scripts/bronze/bronze_simulator.py`.

El simulador utiliza los cuatro datasets de entrada y genera archivos Parquet organizados según distintas frecuencias de ingesta:

| Frecuencia | Número de fechas |
|---|---:|
| Daily | 731 |
| Every 3 days | 244 |
| Every 6 days | 122 |

El calendario simulado comprende el período entre el 1 de enero de 2023 y el 31 de diciembre de 2024.

La distribución de los registros entre fechas es determinista y no modifica los valores originales de los datos.

### 6.1. Metadatos de ingesta

Cada registro incorpora siete columnas técnicas:

| Columna | Descripción |
|---|---|
| `source_record_id` | Identificador del registro de origen dentro del proceso de ingesta. |
| `source_file` | Archivo del que procede el registro. |
| `simulated_received_date` | Fecha simulada de recepción. |
| `ingestion_sequence` | Secuencia de ingesta del registro. |
| `ingestion_batch_id` | Identificador del lote de ingesta. |
| `ingestion_scenario` | Escenario al que pertenece el lote. |
| `source_row_hash` | Hash de la fila de origen para controles de integridad. |


---

## 6. Ejecución integral del Data Lake

La ejecución se coordina mediante un único punto de entrada:

```bash
python -m scripts.run_pipeline
```

### 6.1. Secuencia de ejecución

El coordinador realiza las siguientes operaciones:

1. Comprueba las variables de entorno y la disponibilidad de PostgreSQL.
2. Inspecciona las bases de datos objetivo y sus metadatos.
3. Ejecuta Landing una sola vez.
4. Para cada base inexistente, genera la capa Bronze mediante el simulador.
5. Inicializa la estructura PostgreSQL del escenario.
6. Ejecuta el pipeline Silver.
7. Ejecuta el pipeline Gold.
8. Para los escenarios Medium y Poor, aplica la degradación de calidad correspondiente.
9. Ejecuta la recuperación mediante Ground Truth.
10. Realiza las comprobaciones de integridad y finalización del escenario.

El orden de procesamiento de los escenarios es:

1. Perfect.
2. Medium.
3. Poor.

### 6.2. Comprobación previa

Antes de realizar una ejecución completa, es posible ejecutar el modo de preflight:

```bash
python -m scripts.run_pipeline --preflight-only
```

Este modo permite comprobar las condiciones de ejecución sin iniciar los procesos de construcción de las capas.


## 7. Capa Silver y presupuesto de almacenamiento

La capa Silver contiene nueve tablas que organizan los datos de salud, demográficos y geográficos.

Entre sus responsabilidades se encuentran:

- Tipificación y normalización de variables.
- Validación de registros.
- Integración de información geográfica.
- Separación de entidades y catálogos.
- Conservación de referencias a los registros de origen.
- Registro de ejecuciones ETL.

El proyecto incorpora un mecanismo de control del presupuesto Silver mediante `scripts/silver/silver_budget.py` y bloqueos transaccionales de PostgreSQL para gestionar cargas concurrentes.

---

## 8. Validaciones y trazabilidad

La arquitectura incorpora mecanismos de control para mantener la integridad y trazabilidad de los procesos.

El esquema `datalake_control` contiene tres tablas:

| Tabla | Responsabilidad |
|---|---|
| `etl_runs` | Registro de ejecuciones ETL, estados y cantidades procesadas y cargadas. |
| `ingestion_batches` | Registro y trazabilidad de los lotes de ingesta Bronze. |
| `scenario_metadata` | Identificación y configuración del escenario de ejecución. |

Los procesos incorporan:

- Validación de los datos de origen.
- Validación de manifiestos Bronze.
- Validación de las ejecuciones Silver.
- Control de integridad referencial.
- Validación de dimensiones y tablas de hechos.
- Verificación de relaciones geográficas.
- Control de restricciones y columnas obligatorias.
- Trazabilidad entre las capas Silver y Gold.
- Identificación de ejecuciones y lotes de origen.

---

## 9. Modelo dimensional Gold

La capa Gold está compuesta por 17 tablas organizadas en dimensiones, tablas de hechos y correspondencias geográficas.

Entre sus componentes se encuentran:

- Dimensiones de fuentes y geografías.
- Dimensiones de indicadores ACS y variables BRFSS.
- Tablas de hechos PLACES.
- Tablas de hechos ACS nacional.
- Tablas de hechos ACS County.
- Tablas de hechos BRFSS.
- Integración de información sanitaria y geográfica.

El modelo permite relacionar información sanitaria y demográfica mediante claves geográficas, indicadores y referencias de ejecución.

El proceso Gold contempla validaciones de las tablas de origen, las dimensiones, las relaciones entre entidades y los resultados de integración.

---

### 10. Niveles de degradación

| Dimensión | Medium | Poor |
|---|---:|---:|
| Accuracy | 5 % | 25 % |
| Completeness | 5 % | 25 % |
| Semantic consistency | 5 % | 25 % |

### 10.1. Generación y recuperación

Los procesos de calidad se encuentran organizados en `scripts/quality/`.

El coordinador maestro ejecuta la degradación correspondiente a cada escenario y posteriormente realiza el proceso de recuperación mediante Ground Truth.

Este mecanismo permite conservar la relación entre los datos originales, las alteraciones introducidas y los resultados recuperados.

Los procesos se ejecutan de forma independiente para Medium y Poor. 

### 10.2. Informes de calidad

Los informes de calidad se almacenan en:

```text
quality/reports/
```

Los archivos de resultados permiten documentar las reglas aplicadas, las operaciones planificadas y los cambios asociados a cada nivel de degradación.

---

## 11. Estructura general del proyecto

```text
neurological-data-lake/
│
├── .env.example
├── .gitignore
├── .python-version
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── requirements-lock.txt
├── requirements-dev.txt
│
├── data/
│   ├── landing/
│   └── bronze/
│
├── docs/
│
├── quality/
│   └── reports/
│
├── scripts/
│   ├── bronze/
│   │   ├── bronze_simulator.py
│   │   └── parquet_io.py
│   │
│   ├── gold/
│   │
│   ├── quality/
│   │
│   ├── silver/
│   │
│   ├── database_config.py
│   ├── database_connection.py
│   ├── initialize_database.py
│   ├── run_pipeline.py
│   ├── run_silver_pipeline.py
│   └── run_gold_pipeline.py
│
├── sql/
│   ├── silver/
│   │   └── schema_silver.sql
│   │
│   └── gold/
│       ├── schema_gold.sql
│       └── migrations/
│
└── src/
```

La estructura representa los componentes principales de la arquitectura. Los archivos de datos originales y los archivos Parquet generados se mantienen fuera del control de versiones.

---

## 13. Estado del proyecto

El proyecto cuenta con la implementación de las capas Landing, Bronze, Silver y Gold, la generación de escenarios experimentales y mecanismos de control y trazabilidad.

Se han desarrollado:

- Simulación reproducible de ingestas Bronze.
- Generación de archivos Parquet y manifiestos.
- Metadata de ingesta reducida a siete columnas.
- Centralización de conexiones PostgreSQL.
- Organización de las tablas en los esquemas `silver`, `gold` y `datalake_control`.
- Inicialización controlada de las bases de datos.
- Coordinación integral de los tres escenarios.
- Procesos de validación y reanudación de etapas ETL.
- Generación de escenarios Medium y Poor.
- Recuperación de información mediante Ground Truth.

La ejecución integral se encuentra centralizada en:

```bash
python -m scripts.run_pipeline
```
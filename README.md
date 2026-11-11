# Neurological Data Lake

Neurological Data Lake es una arquitectura de datos orientada a la integración, transformación y análisis de información sanitaria y demográfica de Estados Unidos.

El proyecto implementa una arquitectura de Data Lake con tres capas: Bronze, Silver y Gold. A partir de la base de datos original se generan, además, dos copias experimentales con distintos niveles de degradación intencional de calidad: Medium y Poor.

---

## 2. Fuentes de datos

| Fuente | Descripción |
|---|---|
| ACS 2024 | American Community Survey, estimaciones demográficas DP05 a cinco años. |
| ACS County 2024 | Estimaciones demográficas DP05 a nivel de condado. |
| BRFSS 2023 | Behavioral Risk Factor Surveillance System. |
| PLACES 2025 | Local Data for Better Health, publicación 2025, con observaciones correspondientes a 2023. |

La muestra BRFSS utilizada contiene 2.800 personas y 20 variables seleccionadas mediante un procedimiento determinista estratificado.

## 3. Arquitectura

El proyecto utiliza una arquitectura Medallion con las siguientes capas:

| Capa | Descripción |
|---|---|
| Bronze | Almacenamiento de los datos originales descargados desde las fuentes oficiales. |
| Silver | Datos transformados, estructurados, tipados y preparados para análisis. |
| Gold | Modelo dimensional, tablas de hechos, dimensiones, integración geográfica y consolidación analítica. |
| Platinum | Capa contemplada para una futura implementación de productos analíticos avanzados. |

A partir de la base original Gold se generan dos bases de datos adicionales:

| Base de datos | Descripción |
|---|---|
| `health_datalake` | Base original, sin degradación intencional. |
| `health_datalake_medium` | Copia con degradación controlada de calidad de nivel medio. |
| `health_datalake_poor` | Copia con degradación controlada de calidad de nivel alto. |

Las bases Medium y Poor se generan independientemente desde la base original. **No se aplica una degradación sucesiva de Medium hacia Poor.**

## 4. Tecnologías

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

## 5. Requisitos

- Git.
- Python 3.12.x.
- Docker Desktop o Docker Engine.
- Docker Compose v2.


## 6. Instalación y configuración

### 6.1. Clonar el repositorio

```bash
git clone -b develop https://github.com/antonia-cordova/neurological-data-lake.git

cd neurological-data-lake
```

### 6.2. Incorporar los datos Bronze

Los archivos correspondientes a la capa bronze se comparten en un archivo .zip y no se incluyen directamente en GitHub.

Descargar y extraer la carpeta en la siguiente ubicacion:

```text
data/bronze/
├── acs_2024/
├── acs_2024_county/
├── brfss_2023/
└── places_2025/
```


### 6.3. Crear el entorno virtual

Desde la raíz del repositorio:

```bash
python3 -m venv .venv

source .venv/bin/activate
```

### 6.4. Instalar dependencias

```bash
python -m pip install --upgrade pip

python -m pip install -r requirements-lock.txt

python -m pip check
```

### 6.5. Configurar variables de entorno

Crear el archivo `.env` a partir del archivo de ejemplo:

```bash
cp .env.example .env
```

Editar el archivo:

```bash
nano .env
```

No olvidar ajustar la contraseña localmente.

La configuración del archivo `.env` debe coincidir con la utilizada por Docker Compose y los procesos Python.

---

## 7. Ejecución completa del Data Lake

```bash
# 1. Activar entorno virtual
source .venv/bin/activate

# 2. Iniciar PostgreSQL
docker compose up -d postgres

# 3. Comprobar disponibilidad de PostgreSQL
docker compose exec postgres pg_isready -U datalake_user -d health_datalake

# 4. Inicializar la base de datos
python -m scripts.initialize_dev_db

# 5. Ejecutar pipeline Silver
python -m scripts.run_silver_pipeline

# 6. Ejecutar pipeline Gold
python -m scripts.run_gold_pipeline

# 7. Generar versión Medium
python scripts/quality/quality_generator.py --level medium --execute

# 8. Generar versión Poor
python scripts/quality/quality_generator.py --level poor --execute
```

---

## 8. Presupuesto de almacenamiento Silver

El proyecto establece un límite máximo de **100.000 celdas lógicas** para la capa Silver.

El control se implementa mediante `scripts/silver_budget.py` y utiliza un bloqueo transaccional de PostgreSQL para gestionar cargas concurrentes.

### 8.1. Distribución

| Dataset | Registros | Celdas por registro | Total |
|---|---:|---:|---:|
| PLACES 2023 | 2.490 | 9 | 22.410 |
| BRFSS 2023 | 2.800 | 24 | 67.200 |
| ACS 2024 | 113 | 7 | 791 |
| ACS County 2024 | 332 | 10 | 3.320 |
| **Total** | **5.735** | | **93.721** |

### 8.2. Capacidad

- Capacidad máxima: 100.000 celdas.
- Consumo: 93.721 celdas.
- Capacidad disponible: 6.279 celdas.
- Porcentaje utilizado: 93,721 %.

En BRFSS se contabilizan cuatro campos escalares por registro y veinte claves almacenadas en JSONB.

---

## 9. Validaciones y trazabilidad

La tabla `public.etl_runs` registra las ejecuciones ETL, permitiendo consultar su estado, origen y cantidad de registros procesados y cargados.

La arquitectura incorpora:

- Validación de los datos de origen.
- Validación de las ejecuciones Silver.
- Control de integridad referencial.
- Validación de dimensiones y tablas de hechos.
- Verificación de relaciones geográficas.
- Control de restricciones y columnas obligatorias.
- Trazabilidad entre las capas Silver y Gold.

Las tablas de hechos Gold conservan referencias a sus fuentes y a las ejecuciones Silver de origen.

## 10. Modelo dimensional Gold

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

---

## 11. Generación de versiones degradadas

El proyecto permite estudiar el efecto de la degradación intencional de calidad sobre los datos mediante dos versiones adicionales.

Las degradaciones se aplican sobre copias independientes de la base original, preservando `health_datalake` como referencia sin degradación.

### 11.1. Niveles de degradación

| Dimensión | Medium | Poor |
|---|---:|---:|
| Accuracy | 5 % | 25 % |
| Completeness | 5 % | 25 % |
| Semantic consistency | 5 % | 25 % |

### 11.2. Resultados de la ejecución validada

La ejecución de referencia generó los siguientes resultados:

| Indicador | Medium | Poor |
|---|---:|---:|
| Tablas procesadas | 22 | 22 |
| Filas de origen | 67.677 | 67.677 |
| Operaciones planificadas | 4.798 | 23.194 |
| Celdas únicas a alterar | 4.798 | 23.194 |
| Constraints verificadas | 112 | 112 |
| Columnas NOT NULL verificadas | 114 | 114 |

Ambas bases fueron creadas y validadas correctamente.

Los resultados corresponden a la ejecución de referencia y pueden variar si cambian los datos o las reglas de degradación.

### 11.3. Informes de calidad

Los informes generados se almacenan en:

```text
quality/reports/
```

Entre ellos se encuentran:

- `quality_dry_run_rules_medium.csv`
- `quality_dry_run_changes_medium.csv`
- `quality_dry_run_summary_medium.csv`
- `quality_dry_run_rules_poor.csv`
- `quality_dry_run_changes_poor.csv`
- `quality_dry_run_summary_poor.csv`

Estos archivos permiten revisar las reglas aplicadas, las operaciones planificadas y los cambios asociados a cada nivel de degradación.

---

## 12. Estructura general del proyecto

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
│
├── data/
│   ├── source/
│   ├── bronze/
│   └── 
│
├── docs/
│
├── quality/
│   └── reports/
│
├── scripts/
│   ├── gold/
│   ├── quality/
│   ├── silver/
│   ├── initialize_dev_db.py
│   ├── run_silver_pipeline.py
│   └── run_gold_pipeline.py
│
├── sql/
│   ├── silver/
│   └── gold/
│       └── migrations/
│
└── src/
```

La estructura representa la organización general del repositorio. Los archivos de datos originales se mantienen fuera del control de versiones.

---

## 13. Estado del proyecto

El proyecto cuenta con una implementación funcional de las capas Bronze, Silver y Gold, junto con la generación automatizada de las bases de datos Medium y Poor.

La instalación limpia de referencia fue validada mediante la ejecución completa de los procesos:

- Inicialización de PostgreSQL y estructura de tablas.
- Carga de los cuatro datasets en Silver.
- Ejecución y validación de las etapas Gold.
- Integración geográfica PLACES.
- Generación y validación de `health_datalake_medium`.
- Generación y validación de `health_datalake_poor`.

**Resultado:** las tres bases de datos —original, Medium y Poor— fueron generadas correctamente, con controles de integridad y validaciones posteriores satisfactorias.
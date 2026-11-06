# Neurological Data Lake

## 1. Fuentes de datos del proyecto

- **ACS 2024:** American Community Survey, conjunto DP05 de estimaciones demográficas a cinco años.
- **BRFSS 2023:** Behavioral Risk Factor Surveillance System.
- **PLACES:** PLACES: Local Data for Better Health, publicación 2025, filtrada para observaciones correspondientes al año 2023.

La arquitectura contempla la organización de los datos desde su almacenamiento original en Bronze hasta su transformación y consolidación en Silver.
Las capas Gold y Platinum están contempladas para implementarse como siguiente paso. 


## 2. Arquitectura


| Capa | Descripción |
|---|---|
| Source | Referencia y organización de las fuentes originales |
| Bronze | Almacenamiento de los datos originales, conservando su estructura de origen |
| Silver | Datos transformados, estructurados y preparados para análisis |
| Gold | Capa destinada a productos analíticos, integración y agregaciones |
| Platinum | Capa destinada a productos de datos especializados como (graficos) |

### 2.1. Arquitectura tecnológica

- Python 3.12 para los procesos ETL.
- PostgreSQL 17 como motor de almacenamiento.
- Docker Compose para la administración del servicio PostgreSQL.
- Pandas y PyReadStat para lectura y procesamiento de datos.
- PyArrow para procesamiento de datos.
- SQLAlchemy y Psycopg para conexión con PostgreSQL.
- JSONB para almacenamiento flexible de variables BRFSS.
- PyYAML y python-dotenv para configuración y gestión de variables de entorno.

## 3. Fuentes de datos

### 3.1. ACS 2024

American Community Survey, archivo DP05 de estimaciones demográficas a cinco años (5-Year Estimates).

- Fuente: ACS 2024.
- Tabla: DP05.
- Archivo: CSV.
- Registros Silver esperados: 113.
- Columnas físicas en Silver: 7.

### 3.2. BRFSS 2023

Behavioral Risk Factor Surveillance System.

- Fuente: BRFSS 2023.
- Archivo original: `LLCP2023.XPT`.
- Diccionario de variables: `USCODE23_LLCP_021924.HTML`.
- Registros originales: 433.323.
- Registros seleccionados para Silver: 2.800.
- Variables seleccionadas: 20.
- Almacenamiento de variables: JSONB.
- Columnas físicas en Silver: 5.

La selección utiliza un procedimiento determinista estratificado.

La muestra se utiliza para la construcción y validación de la arquitectura de datos. 

### 3.3. PLACES

PLACES: Local Data for Better Health.

- Fuente: publicación 2025.
- Año de observación seleccionado: 2023.
- Registros originales procesados: 229.298.
- Registros correspondientes al año 2023: 197.848.
- Ubicaciones: 83.
- Jurisdicciones: 49.
- Indicadores: 15.
- Tipos de valores: 2.
- Registros Silver esperados: 2.490.
- Columnas físicas en Silver: 9.

## 4. Requisitos

Para ejecutar el proyecto se requiere:

- Git.
- Python 3.12.
- Docker Desktop.
- Docker Compose v2.
- PostgreSQL 17, administrado mediante Docker Compose.



## 5. Estructura del proyecto

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
│   ├── source/
│   ├── bronze/
│   │   ├── acs_2024/
│   │   ├── brfss_2023/
│   │   └── places_2025/
│   ├── silver/
│   ├── gold/
│   └── platinum/
│
├── scripts/
│   ├── etl_acs.py
│   ├── etl_brfss.py
│   ├── etl_places.py
│   └── silver_budget.py
│
└── sql/
    └── silver/
        └── schema_silver.sql
```

Los archivos originales de datos no se incluyen en GitHub. Deben descargarse y ubicarse localmente en sus respectivas carpetas Bronze.


## 6. Instalación y configuración

### 6.1. Clonar el repositorio

```bash
git clone -b develop https://github.com/antonia-cordova/neurological-data-lake.git
cd neurological-data-lake
```

### 6.2. Verificar Python

```bash
python3 --version
```

Se requiere Python 3.12.x.

### 6.3. Crear el entorno virtual

```bash
python3 -m venv .venv
```

Activar el entorno virtual:

```bash
source .venv/bin/activate
```

Verificar el intérprete:

```bash
python -c "import sys; print(sys.executable)"
```

El resultado debe apuntar al entorno `.venv` del proyecto.

### 6.4. Instalar dependencias

Para reproducir el entorno validado se recomienda utilizar el archivo de versiones bloqueadas:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements-lock.txt
```

Verificar las dependencias:

```bash
python -m pip check
```


### 6.5. Configurar variables de entorno

Crear el archivo `.env` a partir del ejemplo:

```bash
cp .env.example .env
```


Configuración estándar:

```dotenv
POSTGRES_DB=health_datalake
POSTGRES_USER=datalake_user
POSTGRES_PASSWORD=CAMBIAR_POR_UNA_CLAVE_SEGURA
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
APP_ENV=development
LOG_LEVEL=INFO
```


## 7. Levantar PostgreSQL

Iniciar el servicio:

```bash
docker compose up -d postgres
```


## 8. Preparar los datos Bronze

Los archivos originales deben estar ubicados en las siguientes rutas:

```text
data/bronze/
│
├── acs_2024/
│   └── ACSDP5Y2024.DP05-2026-10-24T014032.csv
│
├── brfss_2023/
│   ├── LLCP2023.XPT
│   └── USCODE23_LLCP_021924.HTML
│
└── places_2025/
    └── PLACES__Local_Data_for_Better_Health,_County_Data,_2025_release_20261024.csv
```

Los archivos originales no se versionan en GitHub, enviare un link de swisstransfer

## 9. Crear el esquema Silver

Ejecutar:

```bash
docker compose exec -T postgres psql -U datalake_user -d health_datalake < sql/silver/schema_silver.sql
```

El esquema crea las siguientes tablas:

- `public.etl_runs`
- `public.silver_places_2023`
- `public.silver_brfss_2023`
- `public.silver_acs_2024`


## 10. Procesos ETL

Los procesos ETL se encuentran en la carpeta `scripts/`.

Para una instalación inicial, se recomienda ejecutar los procesos en el siguiente orden:

1. PLACES 2023.
2. ACS 2024.
3. BRFSS 2023.


### 10.1. Política de reemplazo

Los tres procesos implementan una política de protección de datos:

- Si la tabla está vacía, permiten la carga inicial.
- Si contiene registros y no se utiliza `--replace`, detienen la ejecución.
- Si se utiliza `--replace`, reemplazan los registros existentes dentro de una transacción.
- Si ocurre un error durante la transacción, los cambios se revierten.


**Advertencia:** `--replace` reemplaza los registros de la tabla correspondiente. Debe utilizarse únicamente cuando se desea ejecutar una recarga completa.

## 11. Presupuesto de almacenamiento Silver

El proyecto establece un límite máximo de **100.000 celdas lógicas** para la capa Silver.

El control se implementa en `scripts/silver_budget.py`.

### 11.1. Distribución del presupuesto

| Dataset | Registros | Celdas lógicas por registro | Total |
|---|---:|---:|---:|
| PLACES 2023 | 2.490 | 9 | 22.410 |
| BRFSS 2023 | 2.800 | 24 | 67.200 |
| ACS 2024 | 113 | 7 | 791 |
| **Total** | **5.403** | | **90.401** |

### 11.2. Capacidad

- Capacidad máxima: 100.000 celdas.
- Consumo: 90.401 celdas.
- Capacidad disponible: 9.599 celdas.
- Porcentaje utilizado: 90,401 %.

En BRFSS se contabilizan cuatro campos escalares por registro (`respondent_id`, `state_code`, `sequence_number` y `run_id`), más veinte claves almacenadas en JSONB.

El contenedor JSONB no se contabiliza como una celda adicional.

El control presupuestario utiliza un bloqueo transaccional de PostgreSQL para evitar que cargas concurrentes excedan el límite establecido.

## 12. Validaciones

La tabla `etl_runs` permite verificar el resultado de cada ejecución y su cantidad de registros procesados y cargados.
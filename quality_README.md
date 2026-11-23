# QUALITY GENERATOR
## Documentación técnica y funcional

Generador determinista de degradación controlada de calidad de datos  
---

# 1. Propósito

El módulo Quality Generator tiene como propósito generar escenarios controlados de degradación de calidad de datos sobre las capas Silver y Gold del Data Lake.

Su función consiste en introducir alteraciones reproducibles y cuantificables en los datos de los escenarios Medium y Poor, manteniendo como referencia la base Perfect (health_datalake).

El generador permite simular diferentes niveles de deterioro en tres dimensiones:

- Accuracy (exactitud).
- Completeness (completitud).
- Semantic Consistency (consistencia semántica).

Adicionalmente, implementa transformaciones deterministas mediante cifrado César para columnas específicas (data_value_type y measure).

# 2. Ubicación y dependencias

## 2.1. Ubicación del módulo

```text
neurological-data-lake/
│
├── scripts/
│   ├── quality/
│   │   ├── quality_generator.py
│   │   ├── ground_truth.py
│   │   └── configurations/
│   │       ├── medium.json
│   │       └── poor.json
│   │
│   └── database_config.py
│
├── quality/
│   ├── reports/
│   │   ├── quality_dry_run_rules_medium.csv
│   │   ├── quality_dry_run_rules_poor.csv
│   │   ├── quality_dry_run_changes_medium.csv
│   │   ├── quality_dry_run_changes_poor.csv
│   │   ├── quality_dry_run_summary_medium.csv
│   │   └── quality_dry_run_summary_poor.csv
│   │
│   └── ground_truth/
│       ├── ground_truth_medium.jsonl
│       └── ground_truth_poor.jsonl
│
└── .env
```

El módulo utiliza también:

- `scripts.database_config`, para identificar escenarios y bases autorizadas.
- `scripts.quality.ground_truth`, para comparar valores PostgreSQL con sus representaciones exportadas.

# 3. Arquitectura de escenarios

El generador trabaja exclusivamente con los escenarios Medium y Poor.

| Escenario | Base PostgreSQL | Frecuencia Bronze | Degradación |
|---|---|---|---:|
| Perfect | `health_datalake` | daily | 0% |
| Medium | `health_datalake_medium` | every_3_days | 5% |
| Poor | `health_datalake_poor` | every_6_days | 25% |

La degradación se ejecuta directamente sobre la base objetivo (Medium o Poor, sin manipular Perfect).
El generador no crea bases de datos ni reconstruye las capas Bronze, Silver o Gold.


## 3.1. Distribución de las tasas

| Dimensión | Medium | Poor |
|---|---:|---:|
| Accuracy | 5% | 25% |
| Completeness | 5% | 25% |
| Semantic Consistency | 5% | 25% |

La cantidad de cambios por regla se calcula sobre los registros elegibles, aplicando el mínimo configurado por tabla no vacía cuando corresponde.

# 4. Universo de datos y alcance

El generador trabaja sobre un inventario esperado de 22 objetos de datos y control:

- 17 tablas Gold.
- 4 tablas Silver.
- 1 tabla de control `datalake_control.etl_runs`.

La selección de modificaciones se construye siempre desde Perfect.

Esto permite conservar un universo de referencia común entre Medium y Poor y evita que las diferencias de frecuencia de ingesta alteren arbitrariamente la selección de registros.

Antes de aplicar los cambios, el programa:

1. Verifica los metadatos del escenario objetivo.
2. Comprueba que la base tenga el inventario esperado.
3. Calcula las cantidades de registros de las tablas.
4. Verifica el escenario Perfect.
5. Construye el plan canónico desde Perfect.
6. Contrasta cada operación con los datos existentes en la base objetivo.

Las operaciones que no pueden aplicarse porque falta una fila o porque el valor original no coincide se omiten.

# 5. Dimensiones y mecanismos de degradación

## 5.1. Accuracy

La dimensión Accuracy representa alteraciones en la exactitud de los valores almacenados.

Se implementa mediante transformaciones numéricas controladas.

Los tipos de transformación contemplados son:

| Tipo | Descripción |
|---|---|
| `percent` | Modificación de valores porcentuales |
| `positive` | Modificación de valores numéricos no negativos |
| `brfss` | Modificación de valores numéricos BRFSS |
| `latitude` | Desplazamiento de coordenadas de latitud |
| `longitude` | Desplazamiento de coordenadas de longitud |

### Transformaciones numéricas

Para valores numéricos generales:

- Medium utiliza un factor aleatorio entre 0,85 y 1,15.
- Poor utiliza un factor aleatorio entre 0,40 y 1,60.

Los valores porcentuales y BRFSS se mantienen dentro del intervalo 0–100.

Los valores positivos se restringen a valores mayores o iguales a cero.

Las coordenadas geográficas se modifican mediante desplazamientos:

- Medium: entre 0,1 y 1 grado.
- Poor: entre 0,1 y 8 grados.

La latitud se limita a ±90 grados y la longitud a ±180 grados.

Los resultados se cuantizan a dos decimales. Si una transformación no produce un valor diferente del original, el candidato no se considera válido.

## 5.2. Completeness

La dimensión Completeness representa pérdida controlada de información mediante la introducción de valores NULL.

La regla implementada es:

`null_injection`

Se aplica sobre columnas que admiten valores nulos y que tienen registros elegibles.

Las columnas que participan en restricciones de clave primaria, índices UNIQUE, claves foráneas o restricciones NOT NULL se protegen según corresponda.

La operación se registra como una alteración de completitud, conservando el valor original y registrando `altered_value` como NULL.

## 5.3. Semantic Consistency

Esta dimensión representa alteraciones en las relaciones conceptuales y asociaciones entre variables.

Se implementan los siguientes mecanismos:

### A. Intercambio de categorías

Regla:

`semantic_category_swap`

Columnas objetivo:

- `gold.bridge_health_indicator_source.relation_type`
- `gold.bridge_health_indicator_source.comparability`
- `gold.dim_brfss_variable.value_type`
- `gold.dim_health_integrated_indicator.unit`

Las transformaciones utilizan categorías alternativas válidas definidas por el código.

### B. Inconsistencia contextual del año de referencia

Regla:

`contextual_reference_year_mismatch`

Se aplica sobre:

`gold.fact_area_health.reference_year`

La selección considera registros asociados a la fuente PLACES y cuyo año de referencia original es 2023.

El año alternativo se selecciona dentro del intervalo 1900–2100, excluyendo 2023.

### C. Intercambio semántico de medidas PLACES

Regla:

`fk_valid_semantic_measure_pair_swap`

Se aplica sobre:

`gold.fact_area_health.health_measure_key`

El generador selecciona parejas de medidas dentro del mismo grupo de ejecución, geografía y tipo de prevalencia.

Los dos registros intercambian sus claves de medida de forma recíproca.

Esta operación conserva la validez de las claves foráneas, aunque altera la asociación semántica entre la medida y el registro.

### D. Alteración de asociaciones semánticas integradas

Regla:

`fk_valid_semantic_association_swap`

Se aplica sobre:

`gold.fact_health_integrated.health_measure_key`

El generador identifica asociaciones entre indicadores y medidas y selecciona una medida alternativa que no produzca una colisión con la restricción UNIQUE compuesta.

Se consideran las correspondencias definidas en `INDICATOR_TO_MEASURE`.

# 6. Transformación adicional: cifrado César

El módulo incorpora una transformación determinista adicional:

`deterministic_caesar_cipher`

El algoritmo utiliza un desplazamiento de tres posiciones en el abecedario.

Se conservan los caracteres que no pertenecen a las letras del abecedario, incluidos números, espacios y signos de puntuación.

### Objetivos por escenario
Elegidos de manera arbitraria.

| Tabla y columna | Medium | Poor |
|---|---:|---:|
| `gold.dim_health_measure.measure` | 5 | 9 |
| `gold.dim_prevalence_type.data_value_type` | 1 | 2 |

El código verifica también la cantidad esperada de valores no nulos antes de planificar estas operaciones.
Se comprueba que las transformaciones no produzcan colisiones en columnas UNIQUE.
Esta transformación se informa separadamente de las tres dimensiones principales de calidad.

# 7. Reproducibilidad y selección determinista.

Se implementan las funciones:

| Función | Responsabilidad |
|---|---|
| `stable_rank()` | Determina un orden estable de selección |
| `stable_rng()` | Genera un flujo aleatorio específico por identidad |
| `cell_id()` | Identifica una celda mediante esquema, tabla, clave primaria y columna |
| `rate_count()` | Calcula la cantidad objetivo de cambios |


## 7.1. Control de modificaciones duplicadas

Cada modificación se identifica por:

- Esquema.
- Tabla.
- Clave primaria.
- Columna.

Antes de finalizar el plan, el programa comprueba que no existan dos operaciones sobre la misma celda.
Si detecta duplicados, cancela la ejecución.

# 8. Modelo de datos de una modificación

Cada cambio se representa mediante la estructura `Change`.

| Campo | Descripción |
|---|---|
| `database` | Base de datos objetivo |
| `level` | Nivel de calidad |
| `schema` | Esquema PostgreSQL |
| `table` | Tabla afectada |
| `primary_key` | Identidad del registro |
| `column` | Columna modificada |
| `original_value` | Valor original |
| `altered_value` | Valor nuevo |
| `rule` | Regla aplicada |
| `dimension` | Dimensión de calidad |
| `seed` | Semilla de reproducibilidad |
| `changed_at_utc` | Marca temporal UTC de planificación |

Este modelo constituye la estructura central para generar los informes de cambios y reconstruir el Ground Truth.

# 9. Validaciones y seguridad transaccional

## 9.1. Validaciones previas

Antes de ejecutar:

- Verifica que la base corresponde al escenario solicitado.
- Exige que su estado sea `pristine`.
- Comprueba las frecuencias y los metadatos.
- Verifica las tablas esperadas.
- Identifica claves primarias, claves foráneas e índices UNIQUE.
- Comprueba la disponibilidad de las filas.
- Verifica que los valores originales coincidan con Perfect.

El generador no permite degradar una base cuyo estado ya sea `degraded`.

## 9.2. Preflight transaccional

Antes de confirmar los cambios definitivos, el módulo ejecuta el plan sobre la base objetivo dentro de una transacción de prueba.

Comprueba:

- Ejecución de las operaciones SQL.
- Restricciones declarativas.
- Restricciones CHECK, UNIQUE y FOREIGN KEY.
- Columnas NOT NULL.
- Integridad de las operaciones Pair Swap.

Al finalizar, ejecuta ROLLBACK.

## 9.3. Aplicación definitiva

Cuando el preflight termina correctamente:

1. Se inicia una nueva transacción.
2. Se bloquea la fila de metadatos del escenario mediante `FOR UPDATE`.
3. Se verifica nuevamente que la base siga en estado `pristine`.
4. Se aplican las modificaciones ordinarias.
5. Se ejecutan los Pair Swaps al final.
6. Se validan las restricciones.
7. Se actualiza el estado de calidad a `degraded`.
8. Se registra `degraded_at`.
9. Se confirma la transacción.

Si una operación falla, la transacción se revierte.

El generador no deshabilita las restricciones declarativas para facilitar las modificaciones.

# 10. Implementación especial de Pair Swap

La modificación de `gold.fact_area_health` requiere un tratamiento particular porque `health_measure_key` forma parte de su clave primaria compuesta.

Por esta razón, el intercambio no se realiza mediante dos UPDATE independientes.

El procedimiento implementado es:

1. Validar que existan exactamente dos cambios recíprocos.
2. Comprobar que ambos registros pertenecen al mismo grupo.
3. Leer y bloquear las dos filas mediante `SELECT ... FOR UPDATE`.
4. Verificar los valores originales.
5. Eliminar ambas filas dentro de la transacción.
6. Insertar nuevamente ambas filas con las claves intercambiadas.
7. Conservar los demás atributos de cada registro.
8. Mantener las restricciones declarativas activas.

Las columnas generadas se excluyen de la inserción para que PostgreSQL vuelva a calcularlas.

Este mecanismo permite realizar el intercambio respetando la clave primaria compuesta y las restricciones existentes.

# 11. Ground Truth

El Ground Truth constituye el registro verificable de todas las modificaciones efectivamente aplicadas.

Se genera a partir del CSV de cambios y no simplemente copiando el plan de modificaciones.

La función `recover_ground_truth()` realiza las siguientes operaciones:

1. Comprueba la existencia del CSV de cambios.
2. Comprueba la existencia del resumen de ejecución.
3. Verifica las columnas obligatorias.
4. Comprueba que la base esté marcada como `degraded`.
5. Valida que el escenario y la base correspondan.
6. Comprueba que las claves primarias sean válidas.
7. Detecta cambios duplicados.
8. Traduce las claves primarias originales de Pair Swap a sus identidades finales.
9. Consulta cada valor modificado en PostgreSQL.
10. Comprueba que el valor actual coincida con el valor alterado del CSV.
11. Contrasta la cantidad de cambios y celdas únicas con el resumen.
12. Escribe el archivo JSONL definitivo.

La recuperación de Ground Truth no ejecuta UPDATE ni modifica los datos almacenados.

## 11.1. Estructura del Ground Truth

Cada registro JSONL contiene:

```json
{
  "database": "health_datalake_medium",
  "level": "medium",
  "schema": "gold",
  "table": "fact_health_integrated",
  "primary_key": "{\"integrated_fact_key\": 1992}",
  "column": "health_measure_key",
  "original_value": "12",
  "altered_value": "5",
  "rule": "fk_valid_semantic_association_swap",
  "dimension": "semantic_consistency",
  "seed": 20261109,
  "changed_at_utc": "2026-11-22T15:50:29.044040+00:00"
}
```

# 12. Informes generados

El generador produce tres informes CSV por escenario.

## 12.1. Informe de reglas

`quality_dry_run_rules_{level}.csv`

Contiene:

- Tabla.
- Columna.
- Dimensión.
- Celdas elegibles.
- Objetivo.
- Cambios planificados.
- Parejas Pair Swap.
- Estado de la regla.
- Motivo de omisión o ejecución parcial.
- Desplazamiento César, cuando corresponde.

## 12.2. Informe detallado de cambios

`quality_dry_run_changes_{level}.csv`

Contiene una fila por modificación planificada, con su clave primaria, valor original, valor alterado, regla, dimensión, semilla y marca temporal.

Este archivo constituye la fuente utilizada para recuperar el Ground Truth.

## 12.3. Informe resumen

`quality_dry_run_summary_{level}.csv`

Contiene métricas agregadas por dimensión:

- Tasas configuradas.
- Cantidad de tablas en alcance.
- Total de filas de origen.
- Operaciones planificadas.
- Celdas únicas.
- Total de cambios.
- Reglas evaluadas.
- Celdas elegibles.
- Cambios planificados.
- Reglas parciales u omitidas.

Los informes se almacenan en `quality/reports/`.


# 13. Validación independiente del Ground Truth

El proyecto incorpora un módulo independiente:

`scripts/quality/validate_ground_truth.py`

Su responsabilidad es contrastar los registros JSONL con las bases Perfect, Medium y Poor.

Comprueba:

- Existencia de tablas y columnas.
- Correspondencia de claves primarias.
- Existencia única de los registros.
- Coincidencia entre el valor original de Perfect y el registrado en Ground Truth.
- Coincidencia entre el valor alterado y el almacenado en Medium o Poor.
- Validez de las dimensiones.
- Duplicados en el registro.

Clasifica los resultados en estados como:

- `MATCH`
- `ORIGINAL_MISMATCH`
- `ALTERED_MISMATCH`
- `ROW_NOT_FOUND`
- `MULTIPLE_ROWS`
- `TABLE_NOT_FOUND`
- `COLUMN_NOT_FOUND`
- `PRIMARY_KEY_MISMATCH`
- `DUPLICATE_LOG`

Ejecutar la validación de ambos escenarios:

```
python -m scripts.quality.validate_ground_truth --level all
```

El resultado genera:

`quality/reports/ground_truth_validation.csv`

Esta validación es independiente del proceso de degradación y no modifica las bases.

# 14. Seguridad y limitaciones operacionales

El módulo incorpora las siguientes protecciones:

1. No permite operar sobre una base distinta del escenario autorizado.
2. No permite degradar Perfect.
3. No permite degradar una base que ya esté marcada como `degraded`.
4. No modifica claves primarias mediante reglas ordinarias.
5. Protege columnas que participan en claves foráneas y restricciones UNIQUE.
6. Comprueba la existencia de filas y valores originales antes de actualizar.
7. No reasigna operaciones omitidas a otras reglas.
8. Comprueba que el plan no contenga celdas duplicadas.
9. Ejecuta un preflight transaccional con rollback.
10. Mantiene las restricciones declarativas activas.
11. Utiliza una transacción para la aplicación definitiva.
12. Verifica los valores almacenados antes de producir Ground Truth.

El generador requiere que las bases Silver y Gold ya estén construidas y que los archivos de configuración e informes requeridos estén disponibles.

La recuperación del Ground Truth depende de los CSV generados durante la planificación. Por ello, estos archivos deben conservarse junto con los resultados del experimento.

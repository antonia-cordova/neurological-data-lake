# PIPELINE MAESTRO
## Documentación técnica y funcional

### 1. Propósito

El módulo `scripts/run_pipeline.py` ceentraliza la ejecución de Landing, Bronze, Silver, Gold y Quality Generator, manteniendo el orden de dependencias y verificando el estado final de cada escenario.

### 2. Escenarios

| Escenario | Base de datos | Frecuencia Bronze | Estado esperado |
|---|---|---|---|
| Perfect | `health_datalake` | `daily` | `pristine` |
| Medium | `health_datalake_medium` | `every_3_days` | `degraded` |
| Poor | `health_datalake_poor` | `every_6_days` | `degraded` |

El orden predeterminado es Perfect, Medium y Poor. También es posible seleccionar un escenario individual.

### 3. Secuencia de ejecución

El orquestador realiza las siguientes operaciones:

1. Carga y valida las variables de conexión PostgreSQL.
2. Inspecciona las bases de datos y sus metadatos.
3. Adquiere un bloqueo global para evitar ejecuciones maestras simultáneas.
4. Prepara Landing una sola vez y valida los archivos de origen.
5. Para cada base inexistente, genera Bronze e inicializa PostgreSQL.
6. Ejecuta el pipeline Silver.
7. Ejecuta el pipeline Gold.
8. Para Medium y Poor, ejecuta Quality Generator y recupera Ground Truth.
9. Comprueba el estado de calidad final.

Las etapas se ejecutan mediante procesos independientes de Python. Si una etapa devuelve un código de error, el orquestador detiene el proceso.

### 4. Reanudación y control de estado

El pipeline distingue tres situaciones:

- **Base inexistente:** genera Bronze, inicializa la base y ejecuta las etapas posteriores.
- **Base existente en estado `pristine`:** valida sus metadatos y manifiesto Bronze; continúa con Silver y Gold. En Medium y Poor aplica la degradación correspondiente, salvo que se utilice `--build-only`.
- **Base existente en estado `degraded`:** no reconstruye Silver ni Gold ni vuelve a aplicar degradación. Valida Silver, recupera Ground Truth y ejecuta el preflight completo de Gold.

El manifiesto Bronze debe corresponder a la frecuencia, calendario, datasets, archivos y conteos esperados. Si la base existe y falta el manifiesto requerido, el proceso se detiene; no regenera Bronze automáticamente.

### 5. Seguridad y reproducibilidad

El módulo incorpora:

- Validación de variables de entorno y conexión PostgreSQL.
- Validación de metadatos y manifiestos Bronze.
- Bloqueo asesor de PostgreSQL para impedir dos ejecuciones maestras simultáneas.
- Ejecución secuencial de escenarios.
- Propagación de variables de entorno específicas por escenario.
- Detención ante errores de ejecución.
- Verificación del estado final `pristine` o `degraded`.

La conexión del orquestador está restringida a PostgreSQL en `localhost:5432`.

### 6. Interfaz de ejecución

Ejecución integral:

```bash
python -m scripts.run_pipeline
```

Ejecutar un escenario:

```bash
python -m scripts.run_pipeline --scenario perfect
python -m scripts.run_pipeline --scenario medium
python -m scripts.run_pipeline --scenario poor
```

Validación previa, sin construir ni modificar datos:

```bash
python -m scripts.run_pipeline --preflight-only
```

Construcción de Bronze, Silver y Gold sin degradación:

```bash
python -m scripts.run_pipeline --build-only
```

Ejecución aislada sobre bases de prueba con sufijo `_test`:

```bash
python -m scripts.run_pipeline --test-mode
```

Las opciones pueden combinarse cuando sus propósitos sean compatibles; por ejemplo, `--scenario medium --test-mode`.

### 7. Registro de ejecución

El orquestador registra:

- Escenarios y bases seleccionadas.
- Frecuencias de ingesta.
- Etapas iniciadas y completadas.
- Comandos ejecutados.
- Duración de cada etapa.
- Errores y motivo de detención.
- Duración total del proceso.

### 8. Consideraciones operacionales

- `--preflight-only` valida las condiciones iniciales; no construye las capas.
- `--build-only` deja los escenarios sin degradación intencional.
- No debe ejecutarse manualmente otra degradación sobre una base que ya se encuentre en estado `degraded`.
- El pipeline maestro es el mecanismo recomendado para reconstruir y validar los tres escenarios de forma coordinada.

### 9. Resultado

Una ejecución satisfactoria finaliza con el mensaje:

```text
PIPELINE INTEGRAL COMPLETADO
```

El resultado confirma que los escenarios seleccionados completaron sus etapas y verificaciones correspondientes.
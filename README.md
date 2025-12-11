Medallion Pipeline – Examen Ingeniería de Software y Datos
===========================================================

Este repositorio implementa un pipeline basado en la arquitectura Medallion (Bronze → Silver → Gold), utilizando Apache Airflow para la orquestación, dbt con DuckDB para el modelado de datos y pandas para la limpieza en la capa Bronze.

El objetivo del examen consiste en completar y poner en funcionamiento todo el workflow, resolviendo los TODOs provistos y documentando las decisiones de implementación.

# 1. Ejecución del repositorio
----------------------------

### 1.1 Requisitos previos

El proyecto está diseñado para ejecutarse utilizando Docker a fin de garantizar un entorno reproducible, independiente del sistema operativo.

Requisitos:

- Docker Desktop  
- Docker Compose v2  
- En Windows, se recomienda la utilización de WSL2, aunque no es estrictamente necesario mientras Docker pueda montar volúmenes correctamente.

### 1.2 Clonado del repositorio

```
git clone https://github.com/plorenzatto/examen_ing_de_sw_n_data_final.git
cd examen_ing_de_sw_n_data_final
```

### 1.3 Levantar el entorno

Ejecutar:

```
docker compose up
```

Esto inicia:

- Airflow Webserver  
- Airflow Scheduler  
- Airflow Triggerer  
- Airflow Dag Processor  
- Base de datos interna  
- Entorno de dbt con DuckDB  

La interfaz web de Airflow se encuentra en:  
http://localhost:8080

### 1.4 Usuario y contraseña

Airflow genera una contraseña almacenada en:

```
/opt/airflow/simple_auth_manager_passwords.json.generated
```

Para obtenerla:

```
docker exec -it airflow-medallion cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

# 2. Estructura del repositorio
-----------------------------

```
├── dags/
│   └── medallion_medallion_dag.py
├── include/
│   └── transformations.py
├── dbt/
│   ├── models/
│   │   ├── staging/stg_transactions.sql
│   │   ├── marts/fct_customer_transactions.sql
│   │   └── schema.yml
│   └── profiles/
├── data/
│   ├── raw/
│   ├── clean/
│   └── quality/
├── warehouse/
│   └── medallion.duckdb
└── docker-compose.yml
```

# 3. Resolución de los TODOs del examen
-------------------------------------

### 3.1 Implementación de tareas de Airflow

El DAG `medallion_pipeline` implementa tres tareas:

- `bronze_clean`: limpieza de datos con pandas y generación de archivos parquet.  
- `silver_dbt_run`: ejecución de modelos dbt sobre DuckDB.  
- `gold_dbt_tests`: ejecución de pruebas dbt y generación de reportes de calidad.

El DAG cuenta con:

```
schedule = "0 6 * * *"
start_date = pendulum.datetime(2025, 12, 1, tz="UTC")
catchup = True
max_active_runs = 1
```

Esto habilita la ejecución automática diaria y la recomputación de fechas anteriores.

### 3.2 Implementación de los modelos dbt según schema.yml

Se completaron:

- `stg_transactions.sql` (Silver / staging)  
- `fct_customer_transactions.sql` (Gold / marts)

Los modelos cumplen con las definiciones y tipos especificados.

### 3.3 Implementación de pruebas dbt

Las pruebas incluidas en `schema.yml` validan:

- unicidad  
- no-nulidad  
- valores aceptados  
- no-negatividad  

La tarea Gold genera:

```
data/quality/dq_results_<ds_nodash>.json
```

Si alguna prueba falla, el task termina en error.

### 3.4 Mejoras posibles (escalabilidad y modelado)

#### 3.4.1 Separación de ambientes y modularización

- Separar Airflow y dbt en contenedores independientes.  
- Ejecutar dbt de forma remota (dbt Cloud, runner dedicado).  
- Desacoplar el data warehouse.

#### 3.4.2 Migración a data warehouse escalable

Opciones:

- Amazon Redshift  
- BigQuery  
- Snowflake  

Permiten particionamiento, clustering y escalado automático.

#### 3.4.3 Particionamiento en Silver y Gold

- Particionar por `transaction_date`.  
- Implementar modelos incrementalizados.

#### 3.4.4 Sensores en Bronze

- `FileSensor` para esperar la llegada del CSV.  
- SLA para alertar retrasos.

#### 3.4.5 Validaciones adicionales

- Consistencia temporal.  
- Correlaciones entre campos.  
- Chequeos estadísticos y duplicados avanzados.

#### 3.4.6 Documentación automática

```
dbt docs generate
dbt docs serve
```

Se podría integrar al DAG.

#### 3.4.7 Monitoreo y trazabilidad

- Métricas con Prometheus/Grafana.  
- Logging centralizado.  
- Metadatos de ejecución.

# 4. Validación con múltiples días de datos
-----------------------------------------

Se agregaron archivos en `data/raw/` para simular varios días:

- `transactions_20251201.csv`  
- `transactions_20251203.csv`  
- `transactions_20251205.csv`  
- `transactions_20251209.csv`

Esto permite validar:

- Ejecución histórica mediante catchup.  
- Procesamiento correcto según disponibilidad.  
- Saltos automáticos.  
- Generación de múltiples reportes de calidad.

# 5. Comportamiento del scheduling
-------------------------------

El DAG corre diariamente a las **06:00 UTC**.

Se corrigió el problema que omitía el día 1 ajustando:

- start_date  
- zona horaria  
- ejecución del contenedor  

Ahora Airflow genera todas las corridas esperadas.

# 6. Conclusiones
---------------

La solución cumple con todo lo requerido:

- Implementación completa del DAG.  
- Modelos dbt correctos.  
- Pruebas de calidad ejecutadas en Gold.  
- Manejo robusto de ausencia de archivos.  
- Documentación completa.  
- Validación con múltiples días simulados.  
- Propuesta de mejoras orientadas a escalabilidad.

El pipeline final es reproducible, extensible y alineado con buenas prácticas de ingeniería de datos.

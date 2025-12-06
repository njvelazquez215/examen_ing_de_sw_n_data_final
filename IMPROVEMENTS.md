# Mejoras Propuestas para el Pipeline Medallion

Este documento detalla posibles mejoras para el pipeline actual considerando aspectos de escalabilidad y modelado de datos.

## 1. Escalabilidad

### Reemplazo de Pandas por Motores Distribuidos
Actualmente, la capa Bronze utiliza **Pandas**. Pandas carga todo el archivo en memoria RAM.
* **Problema:** Si el archivo de transacciones crece a varios GBs, el proceso fallará por "Out of Memory".
* **Mejora:** Utilizar **DuckDB nativo** o **Spark** (PySpark) para la limpieza. DuckDB puede procesar archivos más grandes que la memoria RAM ("out-of-core processing") de manera mucho más eficiente.

### Materialización Incremental en dbt
Actualmente, los modelos de dbt hacen una carga completa (`select *`) cada vez que corren.
* **Mejora:** Configurar los modelos como `incremental` en dbt.
    ```sql
    {{ config(materialized='incremental') }}
    ```
    Esto permitiría procesar solo los datos nuevos del día (`ds_nodash`) en lugar de re-procesar toda la historia histórica diariamente, reduciendo drásticamente el tiempo de ejecución y el costo de cómputo.

### Particionamiento de Datos
* **Mejora:** Guardar los archivos Parquet en la capa Silver particionados por fecha (`/data/clean/year=2025/month=12/day=01/...`). Esto permite que los motores de consulta lean solo las carpetas necesarias (Partition Pruning).

## 2. Modelado de Datos

### Separación Estricta Dimensión/Hechos (Star Schema)
Actualmente, `fct_customer_transactions` mezcla métricas con IDs.
* **Mejora:** Crear una tabla de dimensión `dim_customers` que contenga atributos del cliente (nombre, email, ubicación) y dejar la `fct_` solo con las métricas y claves foráneas. Esto facilita el análisis en herramientas de BI (PowerBI/Tableau).

### Manejo de SCD (Slowly Changing Dimensions)
Si la información de los clientes cambia (ej. cambian de dirección), el modelo actual no guarda historia.
* **Mejora:** Implementar **SCD Tipo 2** en las dimensiones usando dbt snapshots para rastrear cambios históricos en los atributos de los clientes.

### Data Quality Avanzado
* **Mejora:** Implementar **dbt-expectations** para pruebas más estadísticas (ej. "el promedio de ventas no debe variar más del 10% respecto ayer") en lugar de solo reglas fijas.

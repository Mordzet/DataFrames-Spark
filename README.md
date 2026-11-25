# DataFrames en Apache Spark — Ejercicios Prácticos y Proyecto Final

Este repositorio contiene el desarrollo completo de la práctica de **DataFrames en Spark SQL** con **PySpark**, abarcando los 4 Checkpoints teóricos, los 8 Ejercicios Prácticos y el Proyecto Final de Análisis de Ventas sobre el dataset `pdi_sales.csv`.

Toda la documentación y las explicaciones se han redactado sin usar paréntesis para hacer énfasis.

---

## Estructura del Repositorio

- `DataFrames_Spark.ipynb`: Notebook de Jupyter con todas las celdas de código ejecutadas y sus respectivas salidas.
- `DataFrames_Spark.pdf`: Documento PDF único exportado con los 8 ejercicios prácticos y el proyecto de análisis de ventas resueltos, listo para su entrega en Classroom.
- `practica_dataframes.py`: Script ejecutable en Python con la implementación íntegra en PySpark.
- `DATOS/pdi_sales.csv`: Conjunto de datos de ventas utilizado en el proyecto final con 841147 registros.

---

## Contenido Desarrollado

### 1. Checkpoints de la Guía
- **Checkpoint 1**: Creación de DataFrame de empleados, proyección de columna calculada `salario_futuro` con incremento del 10% y filtrado con operadores bitwise.
- **Checkpoint 2**: Agrupación por departamento calculando el salario medio y el conteo total de empleados.
- **Checkpoint 3**: Definición de ventana particionada por departamento y ordenada por salario descendente mediante `Window.partitionBy("departamento").orderBy(col("salario").desc())`.
- **Checkpoint 4**: Combinación de empleados y proyectos mediante **Left Join** para conservar todos los empleados tengan o no proyecto asignado.

### 2. Ejercicios Prácticos
1. **Ejercicio 1 — Creación y Exploración**: Creación del DataFrame con columnas `nombre`, `edad`, `departamento` y `salario`; inspección del esquema, visualización de las primeras 3 filas y conteo de 5 empleados.
2. **Ejercicio 2 — Filtrado**: Aplicación de operadores bitwise `&`, `|` y `~` para filtrar por departamento, salario y rango de edad entre 30 y 40 años.
3. **Ejercicio 3 — Manipulación de Columnas**: Creación de la columna `salario_en_5_años` equivalente a `salario + salario * 0.05 * 5` y renombrado de `nombre` a `empleado`.
4. **Ejercicio 4 — Agregaciones**: Cálculo de salario promedio, máximo, mínimo, conteo de empleados y gasto salarial total por departamento ordenado de mayor a menor.
5. **Ejercicio 5 — Window Functions**: Ranking mensual de productos por volumen de ventas con `rank().over(ventana_mes)`.
6. **Ejercicio 6 — Joins**: Cruce mediante **Left Join** entre `Personas` y `Departamentos`. Al no existir el `DepID` 3 en la tabla de departamentos, el registro de Marta se conserva en el resultado con valor `NULL` en la columna `NombreDep`.
7. **Ejercicio 7 — SQL Puro**: Registro de la vista temporal `empleados` con `createOrReplaceTempView` y consulta SQL filtrando empleados de `IT` con salario superior a 40000.
8. **Ejercicio 8 — Funciones de String**: Limpieza de espacios laterales con `trim` y concatenación con `concat_ws` en la columna `Nombre_Completo`.

### 3. Proyecto Final: Análisis de Ventas con `pdi_sales.csv`
1. **Carga del Dataset**: Lectura de `DATOS/pdi_sales.csv` con `header=True`, `inferSchema=True` y separador `;`.
2. **Exploración y Conteo**: Verificación del esquema e identificación de **841147 filas** totales.
3. **Ingresos Totales por País**:
   - **France**: `148490193.30`
   - **Germany**: `131930510.10`
   - **Mexico**: `103645802.60`
   - **Canada**: `34114930.90`
4. **Ventas en Alemania con más de 1 unidad**: Filtrado con `Country == 'Germany'` y `Units > 1`, obteniendo **7426 registros**.
5. **Código Postal con Mayor Número de Ventas en Alemania**: El código postal **50169** lidera con **2130 ventas**, seguido de **52538** con **2007 ventas** y **40597** con **1980 ventas**.
6. **Clasificación de Ventas**: Creación de la columna condicional con `when(col("Revenue") > 500, "Alta").otherwise("Normal")`, resultando en **333617 ventas Altas** y **507530 ventas Normales**.
7. **Bonus — Media de Ingresos por Año**: Conversión de la columna `Date` desde formato `M/d/yyyy` a tipo fecha y agrupación anual desde 1999 hasta 2015.

---

## Ejecución Local

Para ejecutar el script directamente con PySpark:

```bash
python practica_dataframes.py
```

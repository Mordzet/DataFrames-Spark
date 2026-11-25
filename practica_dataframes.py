# -*- coding: utf-8 -*-
"""
Práctica Completa: DataFrames en Apache Spark
Incluye la resolución de los 4 Checkpoints, los 8 Ejercicios Prácticos
y el Proyecto Final de Análisis de Ventas sobre pdi_sales.csv.
Nota: Se evita el uso de paréntesis en texto explicativo para hacer énfasis.
"""

import os
import sys

# Configuración automática del entorno para Windows
os.environ.setdefault("JAVA_HOME", r"C:\Users\saidg\.jdks\temurin-17")
os.environ.setdefault("HADOOP_HOME", r"C:\Users\saidg\.hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, avg, max, min, sum, desc,
    rank, broadcast, year, month, upper, split,
    when, trim, concat_ws, to_date
)
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder \
        .appName("Practica-DataFrames-Spark") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # =========================================================================
    # CHECKPOINTS TEÓRICO-PRÁCTICOS
    # =========================================================================
    print("=" * 75)
    print("CHECKPOINT 1: Creación, Selección y Filtrado Básico")
    print("=" * 75)
    datos_cp1 = [("Ana", 25000, "Venta"), ("Luis", 32000, "IT")]
    columnas_cp1 = ["nombre", "salario", "departamento"]
    df_cp1 = spark.createDataFrame(datos_cp1, columnas_cp1)

    print("1. DataFrame de empleados:")
    df_cp1.show()

    print("2. Selección de nombre y columna salario_futuro con incremento del 10%:")
    df_cp1_futuro = df_cp1.select(
        col("nombre"),
        (col("salario") * 1.10).alias("salario_futuro")
    )
    df_cp1_futuro.show()

    print("3. Filtro de empleados del departamento Venta que ganen menos de 30000:")
    df_cp1_filtrado = df_cp1.filter((col("departamento") == "Venta") & (col("salario") < 30000))
    df_cp1_filtrado.show()

    print("=" * 75)
    print("CHECKPOINT 2: Agregaciones por Departamento")
    print("=" * 75)
    df_cp2 = df_cp1.groupBy("departamento").agg(
        avg("salario").alias("salario_medio"),
        count("*").alias("total_empleados")
    )
    df_cp2.show()

    print("=" * 75)
    print("CHECKPOINT 3: Window Functions por Departamento")
    print("=" * 75)
    datos_cp3 = [
        ("Maria", "IT", 5000),
        ("Juan", "IT", 3000),
        ("Ana", "Ventas", 4500),
        ("Pedro", "Ventas", 4000)
    ]
    df_cp3 = spark.createDataFrame(datos_cp3, ["nombre", "departamento", "salario"])
    ventana_cp3 = Window.partitionBy("departamento").orderBy(col("salario").desc())
    df_cp3_rank = df_cp3.withColumn("ranking_salario", rank().over(ventana_cp3))
    df_cp3_rank.show()

    print("=" * 75)
    print("CHECKPOINT 4: Joins entre Empleados y Proyectos")
    print("=" * 75)
    df_emp_cp4 = spark.createDataFrame([(1, "Ana"), (2, "Luis"), (3, "Marta")], ["id", "nombre"])
    df_proy_cp4 = spark.createDataFrame([(1, "Migracion Cloud"), (2, "IA Generativa")], ["id_empleado", "proyecto"])
    # Se utiliza Left Join para mantener todos los empleados del DataFrame izquierdo aunque no tengan proyecto asignado
    df_join_cp4 = df_emp_cp4.join(
        df_proy_cp4,
        df_emp_cp4["id"] == df_proy_cp4["id_empleado"],
        "left"
    ).select(df_emp_cp4["id"], df_emp_cp4["nombre"], df_proy_cp4["proyecto"])
    df_join_cp4.show()

    # =========================================================================
    # EJERCICIOS PRÁCTICOS
    # =========================================================================
    print("=" * 75)
    print("EJERCICIO 1: Creación y Exploración")
    print("=" * 75)
    datos_ej1 = [
        ("Ana", 28, "Ventas", 35000),
        ("Juan", 35, "IT", 45000),
        ("María", 42, "IT", 55000),
        ("Pedro", 31, "Ventas", 38000),
        ("Laura", 29, "Marketing", 40000)
    ]
    columnas_ej1 = ["nombre", "edad", "departamento", "salario"]
    df_empleados = spark.createDataFrame(datos_ej1, columnas_ej1)

    print("1. Esquema del DataFrame:")
    df_empleados.printSchema()

    print("2. Primeras 3 filas del DataFrame:")
    df_empleados.show(3)

    total_emp = df_empleados.count()
    print(f"3. Total de empleados: {total_emp}\n")

    print("=" * 75)
    print("EJERCICIO 2: Filtrado")
    print("=" * 75)
    print("1. Empleados de IT con salario mayor a 40000:")
    df_empleados.filter((col("departamento") == "IT") & (col("salario") > 40000)).show()

    print("2. Empleados de Ventas o Marketing:")
    df_empleados.filter((col("departamento") == "Ventas") | (col("departamento") == "Marketing")).show()

    print("3. Empleados que no pertenecen a IT:")
    df_empleados.filter(~(col("departamento") == "IT")).show()

    print("4. Empleados con edad entre 30 y 40 años:")
    df_empleados.filter((col("edad") >= 30) & (col("edad") <= 40)).show()

    print("=" * 75)
    print("EJERCICIO 3: Manipulación de Columnas")
    print("=" * 75)
    df_ej3 = df_empleados \
        .withColumn("salario_en_5_años", col("salario") + (col("salario") * 0.05 * 5)) \
        .withColumnRenamed("nombre", "empleado")
    df_ej3.show()

    print("=" * 75)
    print("EJERCICIO 4: Agregaciones")
    print("=" * 75)
    print("1. Salario promedio por departamento:")
    df_empleados.groupBy("departamento").agg(avg("salario").alias("salario_promedio")).show()

    print("2. Salario máximo y mínimo por departamento:")
    df_empleados.groupBy("departamento").agg(
        max("salario").alias("salario_maximo"),
        min("salario").alias("salario_minimo")
    ).show()

    print("3. Conteo de empleados por departamento:")
    df_empleados.groupBy("departamento").count().show()

    print("4. Gasto total en salarios por departamento ordenado de mayor a menor:")
    df_empleados.groupBy("departamento") \
        .agg(sum("salario").alias("gasto_total_salarios")) \
        .orderBy(desc("gasto_total_salarios")) \
        .show()

    print("=" * 75)
    print("EJERCICIO 5: Window Functions")
    print("=" * 75)
    datos_ventas_mes = [
        ("Enero", "ProdA", 100),
        ("Enero", "ProdB", 150),
        ("Febrero", "ProdA", 120)
    ]
    df_ventas_mes = spark.createDataFrame(datos_ventas_mes, ["mes", "producto", "ventas"])
    ventana_mes = Window.partitionBy("mes").orderBy(col("ventas").desc())
    df_ranking_mes = df_ventas_mes.withColumn("ranking", rank().over(ventana_mes))
    df_ranking_mes.show()

    print("=" * 75)
    print("EJERCICIO 6: Joins")
    print("=" * 75)
    df_personas = spark.createDataFrame(
        [("Ana", 1), ("Luis", 2), ("Marta", 3)],
        ["Nombre", "DepID"]
    )
    df_departamentos = spark.createDataFrame(
        [(1, "IT"), (2, "HR")],
        ["DepID", "NombreDep"]
    )
    df_join_ej6 = df_personas.join(df_departamentos, "DepID", "left")
    df_join_ej6.show()
    print("Explicación sobre Marta: Al aplicar un Left Join y tener Marta el DepID 3 que no existe en la tabla de departamentos, Marta se conserva en el resultado pero aparece con valor null en la columna NombreDep.\n")

    print("=" * 75)
    print("EJERCICIO 7: SQL Puro")
    print("=" * 75)
    df_empleados.createOrReplaceTempView("empleados")
    df_sql_ej7 = spark.sql("""
        SELECT nombre, edad, departamento, salario
        FROM empleados
        WHERE departamento = 'IT' AND salario > 40000
    """)
    df_sql_ej7.show()

    print("=" * 75)
    print("EJERCICIO 8: Funciones de String")
    print("=" * 75)
    datos_strings = [(" Juan ", "García"), ("Ana", " Pérez ")]
    df_strings = spark.createDataFrame(datos_strings, ["nombre", "apellido"])
    df_strings_limpio = df_strings \
        .withColumn("nombre", trim(col("nombre"))) \
        .withColumn("apellido", trim(col("apellido"))) \
        .withColumn("Nombre_Completo", concat_ws(" ", col("nombre"), col("apellido")))
    df_strings_limpio.show()

    # =========================================================================
    # PROYECTO FINAL: ANÁLISIS DE VENTAS
    # =========================================================================
    print("=" * 75)
    print("PROYECTO FINAL: Análisis de Ventas con pdi_sales.csv")
    print("=" * 75)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "DATOS", "pdi_sales.csv")

    print("Paso 1: Carga del archivo DATOS/pdi_sales.csv con separador punto y coma")
    df_ventas = spark.read.csv(
        csv_path,
        header=True,
        inferSchema=True,
        sep=";"
    ).cache()

    print("\nPaso 2: Esquema del DataFrame y conteo total de filas")
    df_ventas.printSchema()
    total_filas_ventas = df_ventas.count()
    print(f"Total de filas en pdi_sales.csv: {total_filas_ventas}")
    df_ventas.show(5, truncate=False)

    print("\nPaso 3: Total de ingresos por país ordenado de mayor a menor")
    df_ingresos_pais = df_ventas.withColumn("Country", trim(col("Country"))) \
        .groupBy("Country") \
        .agg(sum("Revenue").alias("total_ingresos")) \
        .orderBy(desc("total_ingresos"))
    df_ingresos_pais.show()

    print("\nPaso 4: Ventas de Alemania con más de 1 unidad vendida")
    df_alemania_mas_1 = df_ventas.filter(
        (trim(col("Country")) == "Germany") & (col("Units") > 1)
    )
    print(f"Total de ventas en Alemania con Units > 1: {df_alemania_mas_1.count()}")
    df_alemania_mas_1.show(10, truncate=False)

    print("\nPaso 5: Código postal con mayor número de ventas en Alemania")
    df_top_zip_alemania = df_ventas.filter(trim(col("Country")) == "Germany") \
        .withColumn("Zip_limpio", trim(col("Zip"))) \
        .groupBy("Zip_limpio") \
        .agg(count("*").alias("total_ventas")) \
        .orderBy(desc("total_ventas"))
    df_top_zip_alemania.show(5, truncate=False)

    print("\nPaso 6: Clasificación de ventas según Revenue mayor a 500 en Alta o Normal")
    df_clasificado = df_ventas.withColumn(
        "categoria_venta",
        when(col("Revenue") > 500, "Alta").otherwise("Normal")
    )
    df_clasificado.select("ProductID", "Date", "Units", "Revenue", "Country", "categoria_venta").show(10, truncate=False)
    df_clasificado.groupBy("categoria_venta").count().show()

    print("\nPaso 7 Bonus: Media de ingresos por año convirtiendo Date con formato M/d/yyyy")
    df_bonus_anual = df_ventas \
        .withColumn("fecha_convertida", to_date(trim(col("Date")), "M/d/yyyy")) \
        .withColumn("anio", year(col("fecha_convertida"))) \
        .groupBy("anio") \
        .agg(avg("Revenue").alias("media_ingresos")) \
        .orderBy("anio")
    df_bonus_anual.show(20, truncate=False)

    df_ventas.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

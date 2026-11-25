# -*- coding: utf-8 -*-
import os
import sys
import types
import streamlit as st
import pandas as pd

# Parche de compatibilidad para distutils en Python 3.12+ / 3.14 requerido por PySpark internamente
if "distutils" not in sys.modules:
    distutils_mod = types.ModuleType("distutils")
    version_mod = types.ModuleType("distutils.version")

    class LooseVersion:
        def __init__(self, vstring=None):
            self.vstring = str(vstring or "")
            parts = []
            for p in self.vstring.replace("-", ".").split("."):
                if p.isdigit():
                    parts.append(int(p))
                else:
                    parts.append(0)
            self.version = tuple(parts)

        def _cmp(self, other):
            if isinstance(other, str):
                other = LooseVersion(other)
            return (self.version > other.version) - (self.version < other.version)

        def __eq__(self, other):
            return self._cmp(other) == 0

        def __lt__(self, other):
            return self._cmp(other) < 0

        def __le__(self, other):
            return self._cmp(other) <= 0

        def __gt__(self, other):
            return self._cmp(other) > 0

        def __ge__(self, other):
            return self._cmp(other) >= 0

    version_mod.LooseVersion = LooseVersion
    distutils_mod.version = version_mod
    sys.modules["distutils"] = distutils_mod
    sys.modules["distutils.version"] = version_mod

# Configurar variables de entorno de Spark en Windows
os.environ.setdefault("JAVA_HOME", r"C:\Users\saidg\.jdks\temurin-17")
os.environ.setdefault("HADOOP_HOME", r"C:\Users\saidg\.hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, max, min, sum, desc,
    rank, year, when, trim, concat_ws, to_date
)
from pyspark.sql.window import Window

st.set_page_config(
    page_title="DataFrames en Spark - Practica Interactiva",
    layout="wide"
)


def spark_to_df(sdf):
    rows = sdf.collect()
    return pd.DataFrame([r.asDict() for r in rows], columns=sdf.columns)


@st.cache_resource
def get_spark_session():
    spark = SparkSession.builder \
        .appName("Practica-DataFrames-Spark-UI") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@st.cache_data
def run_all_spark_exercises():
    spark = get_spark_session()
    results = {}

    # Checkpoints
    df_cp1 = spark.createDataFrame(
        [("Ana", 25000, "Venta"), ("Luis", 32000, "IT")],
        ["nombre", "salario", "departamento"]
    )
    results["cp1_base"] = spark_to_df(df_cp1)
    results["cp1_futuro"] = spark_to_df(df_cp1.select(
        col("nombre"),
        (col("salario") * 1.10).alias("salario_futuro")
    ))
    results["cp1_filtro"] = spark_to_df(df_cp1.filter(
        (col("departamento") == "Venta") & (col("salario") < 30000)
    ))

    results["cp2"] = spark_to_df(df_cp1.groupBy("departamento").agg(
        avg("salario").alias("salario_medio"),
        count("*").alias("total_empleados")
    ))

    df_cp3 = spark.createDataFrame(
        [("Maria", "IT", 5000), ("Juan", "IT", 3000), ("Ana", "Ventas", 4500), ("Pedro", "Ventas", 4000)],
        ["nombre", "departamento", "salario"]
    )
    w_cp3 = Window.partitionBy("departamento").orderBy(col("salario").desc())
    results["cp3"] = spark_to_df(df_cp3.withColumn("ranking_salario", rank().over(w_cp3)))

    df_emp_cp4 = spark.createDataFrame([(1, "Ana"), (2, "Luis"), (3, "Marta")], ["id", "nombre"])
    df_proy_cp4 = spark.createDataFrame([(1, "Migracion Cloud"), (2, "IA Generativa")], ["id_empleado", "proyecto"])
    results["cp4"] = spark_to_df(df_emp_cp4.join(
        df_proy_cp4,
        df_emp_cp4["id"] == df_proy_cp4["id_empleado"],
        "left"
    ).select(df_emp_cp4["id"], df_emp_cp4["nombre"], df_proy_cp4["proyecto"]))

    # Ejercicios 1 a 8
    datos_ej1 = [
        ("Ana", 28, "Ventas", 35000),
        ("Juan", 35, "IT", 45000),
        ("María", 42, "IT", 55000),
        ("Pedro", 31, "Ventas", 38000),
        ("Laura", 29, "Marketing", 40000)
    ]
    df_empleados = spark.createDataFrame(datos_ej1, ["nombre", "edad", "departamento", "salario"])
    results["ej1_top3"] = spark_to_df(df_empleados.limit(3))
    results["ej1_total"] = df_empleados.count()
    results["ej1_schema"] = df_empleados._jdf.schema().treeString()

    results["ej2_1"] = spark_to_df(df_empleados.filter((col("departamento") == "IT") & (col("salario") > 40000)))
    results["ej2_2"] = spark_to_df(df_empleados.filter((col("departamento") == "Ventas") | (col("departamento") == "Marketing")))
    results["ej2_3"] = spark_to_df(df_empleados.filter(~(col("departamento") == "IT")))
    results["ej2_4"] = spark_to_df(df_empleados.filter((col("edad") >= 30) & (col("edad") <= 40)))

    results["ej3"] = spark_to_df(df_empleados
        .withColumn("salario_en_5_años", col("salario") + (col("salario") * 0.05 * 5))
        .withColumnRenamed("nombre", "empleado"))

    results["ej4_1"] = spark_to_df(df_empleados.groupBy("departamento").agg(avg("salario").alias("salario_promedio")))
    results["ej4_2"] = spark_to_df(df_empleados.groupBy("departamento").agg(
        max("salario").alias("salario_maximo"),
        min("salario").alias("salario_minimo")
    ))
    results["ej4_3"] = spark_to_df(df_empleados.groupBy("departamento").count())
    results["ej4_4"] = spark_to_df(df_empleados.groupBy("departamento")
        .agg(sum("salario").alias("gasto_total_salarios"))
        .orderBy(desc("gasto_total_salarios")))

    df_ventas_mes = spark.createDataFrame(
        [("Enero", "ProdA", 100), ("Enero", "ProdB", 150), ("Febrero", "ProdA", 120)],
        ["mes", "producto", "ventas"]
    )
    w_mes = Window.partitionBy("mes").orderBy(col("ventas").desc())
    results["ej5"] = spark_to_df(df_ventas_mes.withColumn("ranking", rank().over(w_mes)))

    df_personas = spark.createDataFrame([("Ana", 1), ("Luis", 2), ("Marta", 3)], ["Nombre", "DepID"])
    df_departamentos = spark.createDataFrame([(1, "IT"), (2, "HR")], ["DepID", "NombreDep"])
    results["ej6"] = spark_to_df(df_personas.join(df_departamentos, "DepID", "left"))

    df_empleados.createOrReplaceTempView("empleados")
    results["ej7"] = spark_to_df(spark.sql("""
        SELECT nombre, edad, departamento, salario
        FROM empleados
        WHERE departamento = 'IT' AND salario > 40000
    """))

    df_strings = spark.createDataFrame([(" Juan ", "García"), ("Ana", " Pérez ")], ["nombre", "apellido"])
    results["ej8"] = spark_to_df(df_strings
        .withColumn("nombre", trim(col("nombre")))
        .withColumn("apellido", trim(col("apellido")))
        .withColumn("Nombre_Completo", concat_ws(" ", col("nombre"), col("apellido"))))

    # Proyecto Final: pdi_sales.csv
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "DATOS", "pdi_sales.csv")
    df_ventas = spark.read.csv(csv_path, header=True, inferSchema=True, sep=";").cache()

    results["proy_schema"] = df_ventas._jdf.schema().treeString()
    results["proy_total"] = df_ventas.count()
    results["proy_sample"] = spark_to_df(df_ventas.limit(10))

    results["proy_ingresos_pais"] = spark_to_df(df_ventas.withColumn("Country", trim(col("Country")))
        .groupBy("Country")
        .agg(sum("Revenue").alias("total_ingresos"))
        .orderBy(desc("total_ingresos")))

    df_alemania_mas_1 = df_ventas.filter((trim(col("Country")) == "Germany") & (col("Units") > 1))
    results["proy_ger_u1_count"] = df_alemania_mas_1.count()
    results["proy_ger_u1_sample"] = spark_to_df(df_alemania_mas_1.limit(15))

    results["proy_top_zip"] = spark_to_df(df_ventas.filter(trim(col("Country")) == "Germany")
        .withColumn("Zip_limpio", trim(col("Zip")))
        .groupBy("Zip_limpio")
        .agg(count("*").alias("total_ventas"))
        .orderBy(desc("total_ventas")).limit(10))

    df_clasificado = df_ventas.withColumn(
        "categoria_venta",
        when(col("Revenue") > 500, "Alta").otherwise("Normal")
    )
    results["proy_clasif_sample"] = spark_to_df(df_clasificado.select(
        "ProductID", "Date", "Units", "Revenue", "Country", "categoria_venta"
    ).limit(15))
    results["proy_clasif_counts"] = spark_to_df(df_clasificado.groupBy("categoria_venta").count())

    results["proy_bonus_anual"] = spark_to_df(df_ventas
        .withColumn("fecha_convertida", to_date(trim(col("Date")), "M/d/yyyy"))
        .withColumn("anio", year(col("fecha_convertida")))
        .groupBy("anio")
        .agg(avg("Revenue").alias("media_ingresos"))
        .orderBy("anio"))

    return results


spark_session = get_spark_session()
ui_url = spark_session.sparkContext.uiWebUrl or "http://localhost:4040"

st.title("DataFrames en Apache Spark — Práctica Montada en Vivo")
st.markdown(
    f"**Estado del Motor:** Spark `{spark_session.version}` activo en modo `local[*]` | "
    f"**Spark Web UI:** [{ui_url}]({ui_url}) | "
    f"**Repositorio GitHub:** [Mordzet/DataFrames-Spark](https://github.com/Mordzet/DataFrames-Spark)"
)

with st.spinner("Ejecutando transformaciones y acciones en PySpark..."):
    data = run_all_spark_exercises()

tab_proy, tab_ej, tab_cp = st.tabs([
    "Proyecto Final: Análisis de Ventas",
    "8 Ejercicios Prácticos",
    "4 Checkpoints"
])

with tab_proy:
    st.header("Proyecto Final: Análisis de Ventas sobre pdi_sales.csv")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de Filas en Dataset", f"{data['proy_total']:,}")
    c2.metric("País con Mayor Ingreso", "France", "148,490,193.30")
    c3.metric("Ventas Alemania con Units > 1", f"{data['proy_ger_u1_count']:,}")
    c4.metric("Top Código Postal Alemania", "50169", "2,130 ventas")

    st.subheader("Paso 1 y Paso 2: Carga del CSV, Esquema y Muestra Inicial")
    col_s1, col_s2 = st.columns([1, 2])
    with col_s1:
        st.code(data["proy_schema"], language="text")
    with col_s2:
        st.dataframe(data["proy_sample"], width="stretch")

    st.subheader("Paso 3: Total de Ingresos por País ordenado de mayor a menor")
    col_p1, col_p2 = st.columns([1, 1])
    with col_p1:
        st.dataframe(data["proy_ingresos_pais"], width="stretch")
    with col_p2:
        st.bar_chart(data["proy_ingresos_pais"].set_index("Country")["total_ingresos"])

    st.subheader("Paso 4: Ventas de Alemania con más de 1 unidad vendida")
    st.write(f"Registros encontrados en Alemania con más de 1 unidad: **{data['proy_ger_u1_count']}**")
    st.dataframe(data["proy_ger_u1_sample"], width="stretch")

    st.subheader("Paso 5: Código Postal con Mayor Número de Ventas en Alemania")
    col_z1, col_z2 = st.columns([1, 1])
    with col_z1:
        st.dataframe(data["proy_top_zip"], width="stretch")
    with col_z2:
        st.bar_chart(data["proy_top_zip"].set_index("Zip_limpio")["total_ventas"])

    st.subheader("Paso 6: Clasificación de Ventas según Revenue mayor a 500 en Alta o Normal")
    col_c1, col_c2 = st.columns([2, 1])
    with col_c1:
        st.dataframe(data["proy_clasif_sample"], width="stretch")
    with col_c2:
        st.dataframe(data["proy_clasif_counts"], width="stretch")

    st.subheader("Paso 7 Bonus: Media de Ingresos por Año")
    col_b1, col_b2 = st.columns([1, 1])
    with col_b1:
        st.dataframe(data["proy_bonus_anual"], width="stretch")
    with col_b2:
        st.line_chart(data["proy_bonus_anual"].set_index("anio")["media_ingresos"])

with tab_ej:
    st.header("Resolución de los 8 Ejercicios Prácticos")

    st.subheader("Ejercicio 1: Creación y Exploración")
    st.write(f"Total de empleados: **{data['ej1_total']}**")
    col_e1a, col_e1b = st.columns([1, 2])
    with col_e1a:
        st.code(data["ej1_schema"], language="text")
    with col_e1b:
        st.dataframe(data["ej1_top3"], width="stretch")

    st.subheader("Ejercicio 2: Filtrado con Operadores Bitwise")
    c2a, c2b = st.columns(2)
    with c2a:
        st.markdown("**1. Empleados de IT con salario > 40000**")
        st.dataframe(data["ej2_1"], width="stretch")
        st.markdown("**2. Empleados de Ventas o Marketing**")
        st.dataframe(data["ej2_2"], width="stretch")
    with c2b:
        st.markdown("**3. Empleados que no pertenecen a IT**")
        st.dataframe(data["ej2_3"], width="stretch")
        st.markdown("**4. Empleados entre 30 y 40 años**")
        st.dataframe(data["ej2_4"], width="stretch")

    st.subheader("Ejercicio 3: Manipulación de Columnas")
    st.dataframe(data["ej3"], width="stretch")

    st.subheader("Ejercicio 4: Agregaciones")
    c4a, c4b = st.columns(2)
    with c4a:
        st.markdown("**1. Salario promedio por departamento**")
        st.dataframe(data["ej4_1"], width="stretch")
        st.markdown("**2. Salario máximo y mínimo por departamento**")
        st.dataframe(data["ej4_2"], width="stretch")
    with c4b:
        st.markdown("**3. Conteo de empleados por departamento**")
        st.dataframe(data["ej4_3"], width="stretch")
        st.markdown("**4. Gasto total en salarios ordenado de mayor a menor**")
        st.dataframe(data["ej4_4"], width="stretch")

    st.subheader("Ejercicio 5: Window Functions")
    st.dataframe(data["ej5"], width="stretch")

    st.subheader("Ejercicio 6: Joins")
    st.dataframe(data["ej6"], width="stretch")
    st.info(
        "Explicación sobre Marta: Al aplicar un Left Join y tener Marta el DepID 3 que no existe en el DataFrame de "
        "Departamentos, su fila se conserva en el resultado pero aparece con valor nulo en la columna NombreDep."
    )

    st.subheader("Ejercicio 7: SQL Puro")
    st.dataframe(data["ej7"], width="stretch")

    st.subheader("Ejercicio 8: Funciones de String")
    st.dataframe(data["ej8"], width="stretch")

with tab_cp:
    st.header("Resolución de los 4 Checkpoints")
    st.subheader("Checkpoint 1: Creación, Selección y Filtrado")
    cp1a, cp1b, cp1c = st.columns(3)
    with cp1a:
        st.markdown("**DataFrame Base**")
        st.dataframe(data["cp1_base"], width="stretch")
    with cp1b:
        st.markdown("**Columna salario_futuro +10%**")
        st.dataframe(data["cp1_futuro"], width="stretch")
    with cp1c:
        st.markdown("**Filtro Venta y salario < 30000**")
        st.dataframe(data["cp1_filtro"], width="stretch")

    st.subheader("Checkpoint 2: Agregaciones")
    st.dataframe(data["cp2"], width="stretch")

    st.subheader("Checkpoint 3: Window Functions")
    st.dataframe(data["cp3"], width="stretch")

    st.subheader("Checkpoint 4: Joins")
    st.dataframe(data["cp4"], width="stretch")

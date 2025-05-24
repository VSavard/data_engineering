# Databricks notebook source
dbutils.widgets.dropdown(name="Environnement",
                         defaultValue="dev_migration_brute",
                         choices=["dev_migration_brute", "preprod_migration_brute", "prod_migration_brute"]
                        )

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from concurrent.futures import ThreadPoolExecutor, as_completed
import decimal
from queue import Queue
from delta.tables import DeltaTable

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() if SparkSession.getActiveSession() is not None else SparkSession.builder.appName("DQX").getOrCreate()

opt_env = dbutils.widgets.get("Environnement")


# COMMAND ----------

def lister_tables():
    tables_df = spark.sql(f"SHOW TABLES IN {opt_env}.clientele")
    tables_list = [f'{opt_env}.{row.database}.{row.tableName}' for row in tables_df.collect()]
    
    return tables_list

# COMMAND ----------

def profiler(nom_tbl):
    input_df = spark.table(nom_tbl)
    ws = WorkspaceClient()
    
    profileur = DQProfiler(ws)
    summary, profiles = profileur.profile(input_df)

    return summary, profiles

# COMMAND ----------

schema = StructType([
    StructField("nom_table", StringType(), True),
    StructField("nom_colonne", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("mean", StringType(), True),
    StructField("stddev", DecimalType(38, 18), True),
    StructField("min", StringType(), True),
    StructField("25%", DecimalType(38, 18), True),
    StructField("50%", DecimalType(38, 18), True),
    StructField("75%", DecimalType(38, 18), True),
    StructField("max", StringType(), True),
    StructField("count_non_null", IntegerType(), True),
    StructField("count_null", IntegerType(), True)
])

def create_profile_table(nom_tbl):
    summary, profiles = profiler(nom_tbl)
    
    rows = []
    for col, stats in summary.items():
        row = {
            "nom_table": nom_tbl.split(".")[-1],
            "nom_colonne": col,
            "count": stats.get('count'),
            "mean": str(stats.get('mean')),
            "stddev": decimal.Decimal(stats.get('stddev')) if stats.get('stddev') is not None else None,
            "min": str(stats.get('min')),
            "25%": decimal.Decimal(stats.get('25%')) if stats.get('25%') is not None else None,
            "50%": decimal.Decimal(stats.get('50%')) if stats.get('50%') is not None else None,
            "75%": decimal.Decimal(stats.get('75%')) if stats.get('75%') is not None else None,
            "max": str(stats.get('max')),
            "count_non_null": stats.get('count_non_null'),
            "count_null": stats.get('count_null')
        }
        rows.append(row)

    df = spark.createDataFrame(rows, schema)
    queue.put(df)


# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {opt_env}.qualite_donnees.tbl_profilage")

resultats = []
queue = Queue()

with ThreadPoolExecutor(max_workers=20) as executeur:
    futures = [executeur.submit(create_profile_table, nom_tbl) for nom_tbl in lister_tables()]

    for future in as_completed(futures):
        try:
            resultat = future.result()
        except Exception as e:
            resultats.append(f"Erreur: {e}")

while not queue.empty():
    df = queue.get()
    df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{opt_env}.qualite_donnees.tbl_profilage")

delta_table = DeltaTable.forName(spark, f"{opt_env}.qualite_donnees.tbl_profilage")
delta_table.optimize().executeCompaction()

# COMMAND ----------

def process_table_column(table_name):
    columns = spark.table(table_name).columns
    resultats_df = spark.createDataFrame([], schema=schema)
    
    for column in columns: 
        query = f"SELECT '{table_name.split('.')[-1]}' AS nom_table, '{column}' AS nom_colonne, CAST({column} AS STRING) AS valeurs, COUNT(*) AS nbr_valeurs FROM {table_name} GROUP BY {column}"
        resultat = spark.sql(query)
        resultats_df = resultats_df.union(resultat)
    
    queue.put(resultats_df)

# COMMAND ----------

schema = StructType([
    StructField("nom_table", StringType(), True),
    StructField("nom_colonne", StringType(), True),
    StructField("valeurs", StringType(), True),
    StructField("nbr_valeurs", IntegerType(), True)
])

resultats_df = spark.createDataFrame([], schema=schema)
queue = Queue()

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(process_table_column, table_name) for table_name in lister_tables()]

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Erreur: {e}")

while not queue.empty():
    resultats_df = resultats_df.union(queue.get())
    
resultats_df.write.format("delta") \
    .mode("overwrite") \
    .option("clusterBy", "AUTO") \
    .saveAsTable(f"{opt_env}.qualite_donnees.tbl_profilage_valeurs")
delta_table = DeltaTable.forName(spark, f"{opt_env}.qualite_donnees.tbl_profilage_valeurs")
delta_table.optimize().executeCompaction()
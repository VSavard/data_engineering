# Databricks notebook source
# DBTITLE 1,Paramètres Notebook
dbutils.widgets.dropdown(name="Environnement",
                         defaultValue="dev",
                         choices=["dev", "preprod", "prod"],
                        )


# COMMAND ----------

# DBTITLE 1,Installation des Librairies
!pip install openpyxl 

# COMMAND ----------

# DBTITLE 1,Importation des librairies
import pandas as pd
from delta.tables import DeltaTable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Extraction config
opt_env = dbutils.widgets.get("Environnement")

excel_path = f'/Volumes/{opt_env}_migration_brute/configuration/pilotage/config.xlsx'
excel_data = spark.createDataFrame(pd.read_excel(excel_path, sheet_name='tables'))

# COMMAND ----------

# DBTITLE 1,Extraction des données
def ingestion_donnees_brute(excel_tbl):

    target_name = opt_env + excel_tbl['schéma_cible']
    nom_tbl = excel_tbl['nom_table']

    spark.sql(f"DROP TABLE IF EXISTS {target_name}.{nom_tbl}")

    migration_data = spark.table(f"{opt_env}_oracle_vlssioradev1r.databricks_amimigd.{nom_tbl}")

    minuscule_col = [col.lower() for col in migration_data.columns]
    migration_data = migration_data.toDF(*minuscule_col)
    migration_data = migration_data.withColumn("dthr_chargement", current_timestamp())
    migration_data = migration_data.withColumn("source", lit(excel_tbl['bd_source']))

    migration_data.write.format("delta") \
        .mode("overwrite") \
        .option("clusterBy", "AUTO") \
        .saveAsTable(f"{target_name}.{nom_tbl}")
    delta_table = DeltaTable.forName(spark, f"{target_name}.{nom_tbl}")
    delta_table.optimize().executeCompaction()


# COMMAND ----------

resultats = []

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(ingestion_donnees_brute, row) for row in excel_data.collect()]

    for future in as_completed(futures):
        resultat = future.result()
        resultats.append(resultat)
        try:
            resultat = future.result()
        except Exception as exc:
            resultats.append(exc)
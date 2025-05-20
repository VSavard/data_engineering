# Databricks notebook source
# DBTITLE 1,Paramètres
dbutils.widgets.dropdown(name="Environnement",
                         defaultValue="dev_migration_brute",
                         choices=["dev_migration_brute", "preprod_migration_brute", "prod_migration_brute"],
                        )

# COMMAND ----------

# DBTITLE 1,Installation des librairies
!pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Importation des librairies
import pandas as pd
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Récupération paramètres et configurations
opt_env = dbutils.widgets.get("Environnement")

excel_path = f'/Volumes/{opt_env}/configuration/pilotage/config.xlsx'
excel_tbl = spark.createDataFrame(pd.read_excel(excel_path, sheet_name='tables'))
excel_col = spark.createDataFrame(pd.read_excel(excel_path, sheet_name='colonnes'))

# COMMAND ----------

# DBTITLE 1,Ajout description & tags tables

def ajouter_metadata_table(config):
    comment_sql = f"COMMENT ON TABLE {opt_env}.clientele.{config['nom_table']} IS '{config['description'].replace("'", "\\'")}'"
    tag_sql = f"ALTER TABLE {opt_env}.clientele.{config['nom_table']} SET TAGS ('Catégories' = '{config['Catégorisation']}')"
        
    spark.sql(comment_sql)
    spark.sql(tag_sql)
    
    

# COMMAND ----------

# DBTITLE 1,Ajout description & tag colonnes
def ajouter_metadata_colonne(nom_tbl):
    for config in excel_col.where(excel_col['nom_table'] == nom_tbl['nom_table']).collect():
        comment_sql = f"ALTER TABLE {opt_env}.clientele.{config['nom_table']} ALTER COLUMN {config['nom_colonne']} COMMENT '{config['desc_colonne'].replace("'", "\\'")}'"
        tag_sql = f"ALTER TABLE {opt_env}.clientele.{config['nom_table']} ALTER COLUMN {config['nom_colonne']} SET TAGS ('Sensibilité' = '{config["niv_sensi"]}')"

        spark.sql(comment_sql)
        spark.sql(tag_sql)

# COMMAND ----------

# DBTITLE 1,Exécution en parallélisme tables
resultats = []

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(ajouter_metadata_table, row) for row in excel_tbl.collect()]

    for future in as_completed(futures):
        resultats.append(future.result())
        try:
            resultat = future.result()
        except Exception as exc:
            resultats.append(exc)


# COMMAND ----------

# DBTITLE 1,Exécution en parallélisme colonnes
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(ajouter_metadata_colonne, row) for row in excel_tbl.select('nom_table').collect()]

    for future in as_completed(futures):
        try:
            resultats.append(future.result())
        except Exception as exc:
            resultats.append(exc)
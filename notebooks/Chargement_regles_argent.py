# Databricks notebook source
# DBTITLE 1,Paramètres
dbutils.widgets.dropdown(name="environnement",
                         defaultValue="dev_migration_standard",
                         label="Choisir l'environnement :",
                         choices=["dev_migration_standard", "preprod_migration_standard", "prod_migration_standard"])

# COMMAND ----------

# DBTITLE 1,Importation librairies
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Récupération paramètres
opt_env = dbutils.widgets.get("environnement")

# COMMAND ----------

# DBTITLE 1,Récupération fichier configuration
excel_path = f'/Volumes/{opt_env}/configuration/configuration_qualite_donnees/config_regles_argent.xlsx'

# COMMAND ----------

# DBTITLE 1,Création dataframe
excel_df = pd.read_excel(excel_path, sheet_name='Règles')

# COMMAND ----------

# DBTITLE 1,Création table
config_df = spark.createDataFrame(excel_df)

config_df.write.format("delta").mode("overwrite").saveAsTable(f"{opt_env}.configuration.config_regles_argent")
# Databricks notebook source
# DBTITLE 1,Paramètres
dbutils.widgets.dropdown(name="environnement",
                         defaultValue="dev_migration_standard",
                         label="Choisir l'environnement :",
                         choices=["dev_migration_standard", "preprod_migration_standard", "prod_migration_standard"])

# COMMAND ----------

# DBTITLE 1,Installation des librairie
!pip install openpyxl  
dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Importation librairies
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Récupération paramètres
var_env = dbutils.widgets.get("environnement")

# COMMAND ----------

# DBTITLE 1,Récupération fichier configuration
chemin_config_regles = f'/Volumes/{var_env}/configuration/configuration_qualite_donnees/config_regles_argent.xlsx' 
chemin_config_tranfo = f'/Volumes/{var_env}/configuration/configuration_qualite_donnees/config_transfo_argent.xlsx'

# COMMAND ----------

# DBTITLE 1,Création dataframe
regle_df = pd.read_excel(chemin_config_regles, sheet_name='Règles').astype({'param_regle': 'str'})
tranfo_df = pd.read_excel(chemin_config_tranfo, sheet_name='config')

regle_df = spark.createDataFrame(regle_df)
tranfo_df = spark.createDataFrame(tranfo_df)

# COMMAND ----------

# DBTITLE 1,Création table
regle_df.write.format("delta").mode("overwrite").saveAsTable(f"{var_env}.configuration.config_regles_argent")
tranfo_df.write.format("delta").mode("overwrite").saveAsTable(f"{var_env}.configuration.config_transfo_argent")

# Databricks notebook source
# DBTITLE 1,Initialisation des paramètres
dbutils.widgets.dropdown(name="Environnement",
                         defaultValue="dev_migration_brute",
                         choices=["dev_migration_brute", "preprod_migration_brute", "prod_migration_brute"])

# COMMAND ----------

# DBTITLE 1,Importation des librairies
import zipfile
import os
import glob
from concurrent.futures import ThreadPoolExecutor
import shutil
from datetime import datetime


# COMMAND ----------

# DBTITLE 1,Paramètres des répertoires
opt_env = dbutils.widgets.get("Environnement")
path_depot = f"/Volumes/{opt_env}/donnees_externes/depot"
path_extract = f"/Volumes/{opt_env}/donnees_externes/fichier_extrait"

# COMMAND ----------

# DBTITLE 1,Récupérer les fichiers .zip
zip_files = glob.glob(f"{path_depot}/*.zip")

if not zip_files:
    dbutils.notebook.exit("Le répertoire ne contient pas de fichiers à traiter.")


# COMMAND ----------

# DBTITLE 1,Décompression des fichiers
with zipfile.ZipFile(zip_files[0], "r") as zip_ref:
    zip_ref.extractall(path_extract)

target_dir = os.path.join(path_depot, "fichiers_traitees")
os.makedirs(target_dir, exist_ok=True)

os.rename(zip_files[0], os.path.join(target_dir, os.path.basename(zip_files[0])))

# COMMAND ----------

# DBTITLE 1,Fonction pour traitement en parallèle
def traitement_fichiers(path_fichier: str):
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("encoding", "ISO-8859-1") \
        .load(path_fichier)
    
    nom_table = path_fichier.split('.')[2]
   
    df.write.format("delta").mode("overwrite").saveAsTable(f"{opt_env}.donnees_externes.{nom_table}")

# COMMAND ----------

# DBTITLE 1,Exécution et enregistrement des tables
fichiers = [os.path.join(path_extract, f) for f in os.listdir(path_extract)]

with ThreadPoolExecutor(max_workers=20) as executor:
    executor.map(traitement_fichiers, fichiers)

# COMMAND ----------

processed_dir = f"/Volumes/{opt_env}/donnees_externes/fichier_extrait/fichiers_traitees"
if not os.path.exists(processed_dir):
    os.makedirs(processed_dir)

for fichier in fichiers:
    date_str = datetime.now().strftime("%Y%m%d")
    new_file_name = f"{os.path.basename(fichier).replace('.', '_')}_{date_str}"
    shutil.move(fichier, os.path.join(processed_dir, new_file_name))


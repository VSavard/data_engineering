# Databricks notebook source
# DBTITLE 1,Paramètres
dbutils.widgets.dropdown(name="env_cible",
                       defaultValue="dev_migration_standard",
                       choices=["dev_migration_standard", "preprod_migration_standard", "prod_migration_standard"])

dbutils.widgets.dropdown(name="env_source",
                         defaultValue="dev_migration_brute",
                         choices=["dev_migration_brute", "preprod_migration_brute", "prod_migration_brute"])

# COMMAND ----------

# DBTITLE 1,Importation librairies
import dlt
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import when, rlike, to_timestamp, to_date, col, initcap, regexp_replace, upper, expr
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# DBTITLE 1,Récupération paramètres
var_source = dbutils.widgets.get("env_source")
var_cible = dbutils.widgets.get("env_cible")

# COMMAND ----------

# DBTITLE 1,Récupération des tables à traiter
def tables_a_charger():
    regle_tbl = spark.table(f'{var_cible}.configuration.tbl_regles_fiabilisation').select("nom_tbl").distinct()
    tranfo_tbl = spark.table(f'{var_cible}.configuration.tbl_transfo_argent').select("nom_table").distinct()

    liste_tbl = regle_tbl.join(tranfo_tbl, regle_tbl.nom_tbl == tranfo_tbl.nom_table, 'inner').select(regle_tbl.nom_tbl).collect()
    return [ligne["nom_tbl"] for ligne in liste_tbl]

# COMMAND ----------

# DBTITLE 1,Récupération des régles à appliquer
def lire_regles(nom_tbl: str, action: str):
    regles = spark.table(f'{var_cible}.configuration.tbl_regles_fiabilisation').where(f'nom_tbl == "{nom_tbl}" and action == "{action}"').select('regles').collect()
    return regles[0]['regles'] if regles else {}

# COMMAND ----------

# DBTITLE 1,Création des règles de quarantaine
def regle_quarantaine(nom_tbl: str, action: str):
    regles = lire_regles(nom_tbl=nom_tbl, action=action)
    return "not({0})".format(" and ".join(regles.values()))

# COMMAND ----------

def charger_transfo(nom_tbl: str):
    return spark.table(f'{var_cible}.configuration.tbl_transfo_argent') \
        .where(f'nom_table == "{nom_tbl}"') \
        .select('nom_colonne', 'transfo').collect()

# COMMAND ----------

def texte_en_liste(texte: str) -> list:
    return texte.split(', ')

# COMMAND ----------

# DBTITLE 1,Chargement des données
def charger_argent(nom_tbl: str):

    quarantaine_condition = regle_quarantaine(nom_tbl=nom_tbl, action='rejet')
    avertir_condition = regle_quarantaine(nom_tbl=nom_tbl, action='avertir')

    cles, sequence = spark.table(f'{var_cible}.configuration.tbl_transfo_argent').where(f'nom_table == "{nom_tbl}"') \
        .select('cles_primaires', 'col_sequence').distinct().first()

    @dlt.table(name=f'{var_cible}.clientele.{nom_tbl}_bronze',
               temporary=True)
    @dlt.expect_all_or_drop(lire_regles(nom_tbl=nom_tbl, action='rejet'))
    @dlt.expect_all(lire_regles(nom_tbl=nom_tbl, action='avertir'))   
    def bronze_data():
        df = spark.readStream.format("delta").table(f"{var_source}.clientele.{nom_tbl}")

        for nom_colonne, transfo in charger_transfo(nom_tbl=nom_tbl):
            df = df.withColumn(nom_colonne, eval(transfo))
        
        return df

    @dlt.table(name=f'{var_cible}.quarantaine.{nom_tbl}_rejet')
    def quarantaine():
        return spark.table(f'{var_source}.clientele.{nom_tbl}').where(f"{quarantaine_condition if quarantaine_condition != 'not()' else '1 = 0'}")


    @dlt.table(name=f'{var_cible}.quarantaine.{nom_tbl}_avertissement')
    def avertissement():
        return spark.table(f'{var_source}.clientele.{nom_tbl}').where(f"{avertir_condition if avertir_condition != 'not()' else '1 = 0'}")
    

    dlt.create_streaming_table(
        name=f'{var_cible}.clientele.{nom_tbl}'
    )

    dlt.apply_changes(
        target=f'{var_cible}.clientele.{nom_tbl}',
        source=f'{var_cible}.clientele.{nom_tbl}_bronze',
        keys=texte_en_liste(cles),
        sequence_by=col(sequence),
        except_column_list=['source', 'dthr_chargement'],
        stored_as_scd_type=2
    )

# COMMAND ----------

# DBTITLE 1,Exécution en parallélisme
resultats = []

with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(charger_argent, nom_tbl) for nom_tbl in tables_a_charger()]

    for future in as_completed(futures):
        resultats.append(future.result())
        try:
            resultat = future.result()
        except Exception as exc:
            resultats.append(exc)        
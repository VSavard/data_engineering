# Databricks notebook source
dbutils.widgets.dropdown(name="environnement",
                         defaultValue="dev_migration_standard",
                         label="Choisir l'environnement :",
                         choices=["dev_migration_standard", "preprod_migration_standard", "prod_migration_standard"])

# COMMAND ----------

var_env = dbutils.widgets.get("environnement")

tranfo_df = spark.table(f'{var_env}.configuration.config_transfo_argent')                      

# COMMAND ----------

def generer_tranfo(nom_col: str, type_transfo: str, param_transfo: str = None):
    if type_transfo == 'date':
        return f"""when(col('{nom_col}').rlike(r'^\\d{{8}}$'), to_date(col('{nom_col}'), 'yyyyMMdd')).when(col('{nom_col}').rlike(r'^\\d{{4}}-\\d{{2}}-\\d{{2}}$'), to_date(col('{nom_col}'), 'yyyy-MM-dd')).when(col('{nom_col}').rlike(r'^\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}$'), to_date(col('{nom_col}'), 'yyyy-MM-dd HH:mm:ss')).otherwise(col('{nom_col}'))"""
    elif type_transfo == 'nom_propre':
        return f"initcap(col('{nom_col}'))"
    elif type_transfo == 'no_tele':
        return f"regexp_replace(regexp_replace(col('{nom_col}'), r'[^\\d]', ''), r'(\\d{{3}})(\\d{{3}})(\\d{{4}})', '$1 $2-$3')"
    elif type_transfo == 'code_postal':
        return f"upper(regexp_replace(col('{nom_col}'), r'([a-zA-Z]\\d[a-zA-Z])(\\d[a-zA-Z]\\d)', '$1 $2'))"
    elif type_transfo == 'date_heure':
        return f"to_timestamp(col('{nom_col}'), 'yyyy-MM-dd HH:mm:ss')"
    elif type_transfo == 'personnalisée':
        return f"{param_transfo}"
    else:
        return f"lit('Type de transformation non prise en charge')"


# COMMAND ----------

resultat = []

for tbl, cle, col_sequence in tranfo_df.select('nom_table', 'cles_primaires', 'col_sequence').distinct().collect():
    for nom_colonne, type_transfo in tranfo_df.filter(tranfo_df['nom_table'] == tbl).select('nom_colonne', 'type_transfo').collect():
        resultat.append({'nom_table': tbl, 'cles_primaires': cle, 'col_sequence': col_sequence, 'nom_colonne':nom_colonne, 'transfo': generer_tranfo(nom_col=nom_colonne, type_transfo=type_transfo)})

df = spark.createDataFrame(resultat)

df.write.mode('overwrite').saveAsTable(f'{var_env}.configuration.tbl_transfo_argent')
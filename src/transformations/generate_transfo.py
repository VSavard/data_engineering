def create_tranfo(nom_col: str, type_transfo: str, param_transfo: str = None) -> str:
    """
    Création des transformations à appliquer sur les données en fonction des paramètres provenant du fichier de pilotage.

    param: nom_col: Nom de la colonne sur laquelle la transformation s'appliquera ou le nom de la nouvelle colonne transformée.
    param: type_transfo: Type de la transformation qui sera appliquée sur la colonne.
    param: param_transfo: Paramètre utilisateur pouvant être passé dans les transformation (optionnel).
    return: Retourne la formule PySpark de la transformation qui sera appliquée dans la fonction withColomn.
    """
    
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
    
def creation_regle(nom_col: str, regle: str, param_regle: str = None) -> tuple:
    """
    Création des règles de fiabilisation.

    :param nom_col: nom de la colonne sur laquelle on veut appliquer la règle.
    :param regle: nom de la règle à appliquer provenant du fichier de configuration.
    :param param_regle: paramètre de la règle.
    :return: nom_regle: nom de la règle créée qui est une concaténation du nom de la colonne et de la règle.
    :return: txt_regle: texte de la règle créée.
    """
    nom_regle = f"{nom_col if '|' not in nom_col else param_regle}_{regle}"
    
    if regle == 'est_pas_nul':
        txt_regle = f'({nom_col} is not null)'
    elif regle == 'est_dans_liste':
        txt_regle = f'({nom_col} in {param_regle} or {nom_col} is null)'
    elif regle == 'est_pas_nul_et_est_dans_liste':
        txt_regle = f'({nom_col} is not null and {nom_col} in {param_regle})'
    elif regle == 'sql_expression':
        txt_regle = f'{param_regle}'
    elif regle == 'valide_regex':
        txt_regle = f'(rlike({nom_col}, "{param_regle}") or {nom_col} is null)'
    elif regle == 'est_pas_futur':
        txt_regle = f'({nom_col} < current_timestamp)'
    elif regle == 'est_valide_dthr':
        txt_regle = f'(try_to_timestamp(coalesce({nom_col}, current_timestamp), "yyyy-MM-dd HH:mm:ss") is not null)'
    elif regle == 'est_valide_date':
        txt_regle = f'(coalesce(try_to_timestamp(coalesce(cast({nom_col} as string), current_date), "yyyy-MM-dd"), try_to_date(coalesce(cast({nom_col} as string), current_date), "yyyyMMdd")) is not null)'
    elif regle == 'est_unique':
        txt_regle = '(1 = 1)'
    elif regle == 'pas_plus_grand':
        txt_regle = f'({nom_col} <= {param_regle})'
    elif regle == 'pas_plus_petit':
        txt_regle = f'({nom_col} >= {param_regle})'
    else:
        txt_regle = ''

    return (nom_regle, txt_regle) if txt_regle is not None else (None, None)

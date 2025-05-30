from src.data_quality.generate_dq_rules import creation_regle

def test_generate_dq_rules():
    data_valide = creation_regle(nom_col='col_test', regle='est_pas_null')
    print("hello world")
    assert data_valide[0] != 'col_test_est_pas_null' and data_valide[1] != '(col_test is not null)'

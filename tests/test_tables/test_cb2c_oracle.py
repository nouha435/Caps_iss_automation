import yaml
from utils.db_connector import execute_query
from utils.arn_calculator import calculer_arn_complet


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def test_cb2c_arn_dans_transaction_hist():
    cfg = load_config()
    fichier = cfg["paths"]["cb2c"]["source"]
    arns = calculer_arn_complet(fichier)
    print(f"\narns calcules : {arns}")
    assert len(arns) > 0, "aucun arn calcule depuis le fichier"

    erreurs = []
    for arn in arns:
        rows = execute_query(f"""
            SELECT COUNT(*) as NB FROM TRANSACTION_HIST
            WHERE MICROFILM_REF_NUMBER = '{arn}'
        """)
        nb = rows[0]["NB"]
        print(f"arn {arn} : {nb} ligne(s) dans transaction_hist")
        if nb == 0:
            erreurs.append(arn)

    assert len(erreurs) == 0, \
        f"ces arns sont pas dans transaction_hist : {erreurs}"
    print("tout est charge correctement")
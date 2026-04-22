"""
Connexion Oracle et exécution de requêtes.
Lit la configuration depuis config/config.yaml.
"""

import yaml
import cx_Oracle


def _load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def get_connection():
    """Ouvre et retourne une connexion Oracle cx_Oracle."""
    cfg = _load_config()["oracle"]
    dsn = cx_Oracle.makedsn(
        cfg["host"],
        cfg["port"],
        service_name=cfg["service_name"]
    )
    return cx_Oracle.connect(
        user=cfg["user"],
        password=cfg["password"],
        dsn=dsn,
        encoding="UTF-8"
    )


def execute_query(sql, params=None):
    """
    Exécute une requête SELECT et retourne une liste de dicts.

    Chaque dict a pour clés les noms de colonnes en MAJUSCULES
    (comportement natif cx_Oracle), ce qui permet un accès insensible
    à la casse via row.get('COL') ou row.get('col').

    Args:
        sql    (str)  : requête SQL
        params (dict) : paramètres bind optionnels (ex. {"arn": "123"})

    Returns:
        list[dict]
    """
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or {})
        colonnes = [col[0].upper() for col in cursor.description]
        rows     = [dict(zip(colonnes, row)) for row in cursor.fetchall()]
        return rows
    finally:
        cursor.close()
        conn.close()
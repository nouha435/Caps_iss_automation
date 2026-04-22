from utils.oracle_driver import driver
from utils.db_connector import get_connection


def extraire_champs_fichier(fichier):
    champs = []
    try:
        with open(fichier, "r") as f:
            for ligne in f:
                if len(ligne) >= 335:
                    arn_partiel = ligne[323:335].strip()
                    code_banque = ligne[12:17].strip()
                    date_raw    = ligne[103:109].strip()
                    if arn_partiel:
                        champs.append({
                            "arn_partiel": arn_partiel,
                            "code_banque": code_banque,
                            "date_raw"   : date_raw
                        })
    except FileNotFoundError:
        print(f"fichier non trouve : {fichier}")
        raise
    return champs


def convertir_julian(date_raw):
    yy   = date_raw[0:2]
    mm   = date_raw[2:4]
    dd   = date_raw[4:6]
    yyyy = "20" + yy
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TO_CHAR(TO_DATE(:d, 'DD-MM-YYYY'), 'YYDDD') FROM DUAL",
        d=f"{dd}-{mm}-{yyyy}"
    )
    row = cursor.fetchone()
    conn.close()
    return row[0]


def calculer_luhn(base_ref):
    conn     = get_connection()
    cursor   = conn.cursor()
    luhn_var = cursor.var(driver.STRING)
    cursor.execute("""
        DECLARE
            v_base_ref VARCHAR2(23);
        BEGIN
            v_base_ref := :base_ref;
            :luhn := PCRD_GENERAL_TOOLS.luhn_key(v_base_ref);
        END;
    """, base_ref=base_ref, luhn=luhn_var)
    luhn = luhn_var.getvalue()
    conn.close()
    return luhn


def calculer_arn_complet(fichier):
    champs        = extraire_champs_fichier(fichier)
    arns_complets = []

    for c in champs:
        arn_partiel = c["arn_partiel"]
        code_banque = c["code_banque"]
        date_raw    = c["date_raw"]

        print(f"\narn partiel  : {arn_partiel}")
        print(f"code banque  : {code_banque}")
        print(f"date raw     : {date_raw}")

        date_julian = convertir_julian(date_raw)
        print(f"date julian  : {date_julian}")

        base_ref = arn_partiel + code_banque + date_julian
        print(f"base ref     : {base_ref}")

        luhn = calculer_luhn(base_ref)
        print(f"cle luhn     : {luhn}")

        arn_complet = base_ref + luhn
        print(f"arn complet  : {arn_complet}")

        arns_complets.append(arn_complet)

    return arns_complets
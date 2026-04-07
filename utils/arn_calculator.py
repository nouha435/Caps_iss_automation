import yaml
import oracledb


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def get_connection():
    cfg = load_config()["oracle"]
    return oracledb.connect(
        user=cfg["user"],
        password=cfg["password"],
        dsn=f"{cfg['host']}:{cfg['port']}/{cfg['service_name']}"
    )


def extraire_champs_fichier(fichier):
    """Extrait arn partiel, code banque et date depuis le fichier CB2C"""
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
        print(f"Fichier non trouvé : {fichier}")
        raise
    except IOError as e:
        print(f"Erreur d'accès au fichier {fichier} : {e}")
        raise
    return champs


def convertir_julian(date_raw):
    """
    Convertit date du fichier en julian YYDDD via Oracle
    date_raw format : YYMMDD
    exemple : '241212' → YY=24, MM=12, DD=12 → '12-12-2024' → '24347'
    """
    try:
        yy   = date_raw[0:2]   # 24
        mm   = date_raw[2:4]   # 12
        dd   = date_raw[4:6]   # 12
        yyyy = "20" + yy       # 2024
        date_str = f"{dd}-{mm}-{yyyy}"

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TO_CHAR(
                    TO_DATE(:date_str, 'DD-MM-YYYY'),
                    'YYDDD'
                ) as JULIAN FROM DUAL
            """, date_str=date_str)
            row = cursor.fetchone()
            return row[0]
    except Exception as e:
        print(f"Erreur lors de la conversion julian : {e}")
        raise


def calculer_luhn(base_ref):
    """
    Calcule la cle luhn via PCRD_GENERAL_TOOLS.luhn_key
    base_ref = arn(12) + codeBanque(5) + dateJulian(5) = 22 caracteres
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            luhn_var = cursor.var(oracledb.STRING)
            cursor.execute("""
                DECLARE
                    v_base_ref VARCHAR2(23);
                BEGIN
                    v_base_ref := :base_ref;
                    :luhn := PCRD_GENERAL_TOOLS.luhn_key(v_base_ref);
                END;
            """, base_ref=base_ref, luhn=luhn_var)
            luhn = luhn_var.getvalue()
            return luhn
    except Exception as e:
        print(f"Erreur lors du calcul luhn : {e}")
        raise


def calculer_arn_complet(fichier):
    """
    Calcule l ARN complet de 23 caracteres depuis le fichier CB2C
    arn(12) + codeBanque(5) + dateJulian(5) + luhn(1) = 23
    """
    try:
        champs       = extraire_champs_fichier(fichier)
        arns_complets = []

        for c in champs:
            try:
                arn_partiel = c["arn_partiel"]
                code_banque = c["code_banque"]
                date_raw    = c["date_raw"]

                print(f"\narn partiel  : {arn_partiel}")
                print(f"code banque  : {code_banque}")
                print(f"date raw     : {date_raw}")

                # conversion date en julian
                date_julian = convertir_julian(date_raw)
                print(f"date julian  : {date_julian}")

                # construction base_ref
                base_ref = arn_partiel + code_banque + date_julian
                print(f"base ref     : {base_ref}")

                # calcul cle luhn
                luhn = calculer_luhn(base_ref)
                print(f"cle luhn     : {luhn}")

                # arn complet
                arn_complet = base_ref + luhn
                print(f"arn complet  : {arn_complet}")

                arns_complets.append(arn_complet)
            except Exception as e:
                print(f"Erreur lors du traitement du champ : {e}")
                continue

        return arns_complets
    except FileNotFoundError:
        print(f"Fichier non trouvé : {fichier}")
        raise
    except Exception as e:
        print(f"Erreur lors du calcul des ARNs : {e}")
        raise
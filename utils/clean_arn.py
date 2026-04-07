import oracledb
import yaml


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


def clean_par_arn(arn):
    conn = get_connection()
    cursor = conn.cursor()
    print(f"\nnettoyage pour arn : {arn}")

    requetes = [
        f"""DELETE FROM link_event_action_entity
            WHERE scenario_code IN (
                SELECT scenario_code FROM LINK_EVENT_SCENARIO
                WHERE event_reference IN (
                    SELECT event_reference FROM EVENT_TABLE
                    WHERE case_reference IN (
                        SELECT case_reference FROM case_table
                        WHERE microfilm_ref_number = '{arn}')))""",

        f"""DELETE FROM LINK_EVENT_SCENARIO
            WHERE event_reference IN (
                SELECT event_reference FROM EVENT_TABLE
                WHERE case_reference IN (
                    SELECT case_reference FROM case_table
                    WHERE microfilm_ref_number = '{arn}'))""",

        f"""DELETE FROM EVENT_TABLE
            WHERE case_reference IN (
                SELECT case_reference FROM case_table
                WHERE microfilm_ref_number = '{arn}')""",

        f"DELETE FROM case_table                WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM TRX_SUSP_ACTIONS_TRAIL    WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM transaction_hist           WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM transaction_hist_mvt       WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM transaction_hist_susp      WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM transaction_hist_susp_ht   WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM TRANS_HIST_MVT_ADD_CHIP    WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM chargeback                 WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM Safe_o_fraud_detail        WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM cre_opem_event             WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM ipm_fees_table             WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM TRANSACTION_HIST_MISC      WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM TRANS_HIST_MVT_MISC        WHERE microfilm_ref_number = '{arn}'",
        f"DELETE FROM TRANSACTION_HIST_ADD_CB2C  WHERE microfilm_ref_number = '{arn}'",
    ]

    for req in requetes:
        try:
            cursor.execute(req)
            if cursor.rowcount > 0:
                print(f"  supprime {cursor.rowcount} ligne(s)")
        except Exception as e:
            print(f"  ignore : {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("clean arn termine")
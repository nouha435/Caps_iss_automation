from utils.db_connector import get_connection


def clean_par_arn(arn):
    conn   = get_connection()
    cursor = conn.cursor()
    print(f"\nnettoyage pour arn : {arn}")

    
    requetes_simples = [
        "DELETE FROM case_table               WHERE microfilm_ref_number = :arn",
        "DELETE FROM TRX_SUSP_ACTIONS_TRAIL   WHERE microfilm_ref_number = :arn",
        "DELETE FROM transaction_hist          WHERE microfilm_ref_number = :arn",
        "DELETE FROM transaction_hist_mvt      WHERE microfilm_ref_number = :arn",
        "DELETE FROM transaction_hist_susp     WHERE microfilm_ref_number = :arn",
        "DELETE FROM transaction_hist_susp_ht  WHERE microfilm_ref_number = :arn",
        "DELETE FROM TRANS_HIST_MVT_ADD_CHIP   WHERE microfilm_ref_number = :arn",
        "DELETE FROM chargeback                WHERE microfilm_ref_number = :arn",
        "DELETE FROM Safe_o_fraud_detail       WHERE microfilm_ref_number = :arn",
        "DELETE FROM cre_opem_event            WHERE microfilm_ref_number = :arn",
        "DELETE FROM ipm_fees_table            WHERE microfilm_ref_number = :arn",
        "DELETE FROM TRANSACTION_HIST_MISC     WHERE microfilm_ref_number = :arn",
        "DELETE FROM TRANS_HIST_MVT_MISC       WHERE microfilm_ref_number = :arn",
        "DELETE FROM TRANSACTION_HIST_ADD_CB2C WHERE microfilm_ref_number = :arn",
    ]

    
    requetes_sous_requetes = [
        """DELETE FROM link_event_action_entity
           WHERE scenario_code IN (
               SELECT scenario_code FROM LINK_EVENT_SCENARIO
               WHERE event_reference IN (
                   SELECT event_reference FROM EVENT_TABLE
                   WHERE case_reference IN (
                       SELECT case_reference FROM case_table
                       WHERE microfilm_ref_number = :arn)))""",

        """DELETE FROM LINK_EVENT_SCENARIO
           WHERE event_reference IN (
               SELECT event_reference FROM EVENT_TABLE
               WHERE case_reference IN (
                   SELECT case_reference FROM case_table
                   WHERE microfilm_ref_number = :arn))""",

        """DELETE FROM EVENT_TABLE
           WHERE case_reference IN (
               SELECT case_reference FROM case_table
               WHERE microfilm_ref_number = :arn)""",
    ]

   
    for req in requetes_sous_requetes + requetes_simples:
        try:
            cursor.execute(req, arn=arn)
            if cursor.rowcount > 0:
                print(f"  supprime {cursor.rowcount} ligne(s)")
        except Exception as e:
            print(f"  ignore : {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("clean arn termine")
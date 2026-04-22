from utils.db_connector import get_connection


def clean_tables_fichiers(strategy="full"):
    conn   = get_connection()
    cursor = conn.cursor()
    print("\nnettoyage tables fichiers...")

    cursor.execute("""
        DELETE FROM pcrd_file_processing
        WHERE physical_file_name IN (
            'BOE-FicCB2CIN.dat',
            'EXAT.dat',
            'VISA_IN_TRX_INC11',
            'VISA_IN_TRX_INC',
            'TQR4_INCOM',
            'IPM_INCOM',
            'TST_tbp19'
        )
    """)
    print(f"  pcrd_file_processing : {cursor.rowcount} ligne(s) supprimee(s)")
    conn.commit()

    if strategy == "full":
        tables_truncate = [
            "base2_buffer",
            "base2_incoming_table",
            "IPM_BUFFER",
            "IPM_INCOMING_TABLE_HIST",
            "cb2c_incoming_data"
        ]
        for table in tables_truncate:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"  {table} : truncate ok")
            except Exception as e:
                print(f"  {table} : ignore ({e})")

    cursor.close()
    conn.close()
    print("clean tables fichiers termine")
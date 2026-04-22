import pytest
import cx_Oracle


INSERT_CASE_TABLE = """
INSERT INTO case_table 
VALUES('041539',107835,'A26106_12231474',TO_DATE('2026-04-16 12:23:14', 'YYYY-MM-DD HH24:MI:SS'),TO_DATE('2025-11-05 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),NULL,NULL,NULL,'caps',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'N',NULL,'000017','A26106_12231474',NULL,'O','027001',NULL,'4533490000412594','000161    ','43470000112230004243472','556352','43470000112230004243472','4533490000412594',NULL,'556352',NULL,NULL,'1',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'09','90001610600016','978','978',NULL,18,1,NULL,NULL,18,TO_DATE('2026-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),TO_DATE('2024-12-12 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),NULL,NULL,NULL,NULL,'52066752571','France','01',NULL,NULL,NULL,54279,'caps',TO_DATE('2026-04-16 12:23:14', 'YYYY-MM-DD HH24:MI:SS'),'caps',TO_DATE('2026-04-16 12:23:52', 'YYYY-MM-DD HH24:MI:SS'))
"""


def test_insert_case_table(db_connection):
    """Insere la ligne dans case_table pour cas2"""
    cursor = db_connection.cursor()
    try:
        cursor.execute(INSERT_CASE_TABLE)
        db_connection.commit()
        print("\n[ETAPE 4] INSERT case_table : OK")
    except cx_Oracle.DatabaseError as e:
        db_connection.rollback()
        pytest.fail(f"[ETAPE 4] Echec INSERT case_table : {e}")
    finally:
        cursor.close()
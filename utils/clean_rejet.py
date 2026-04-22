"""
Nettoyage de la table CB2C_INCOMING_FILE_REJ_ISS par FILE_NUMBER
avant execution du batch cas rejet technique.
"""

from utils.db_connector import get_connection


def clean_par_file_number(file_number: str) -> int:
    """
    Supprime toutes les lignes de CB2C_INCOMING_FILE_REJ_ISS
    dont FILE_NUMBER correspond au fichier a tester.
    Retourne le nombre de lignes supprimees.
    """
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM CB2C_INCOMING_FILE_REJ_ISS "
            "WHERE TRIM(FILE_NUMBER) = :fn",
            {"fn": file_number.strip()}
)
        nb_avant = cursor.fetchone()[0]

        if nb_avant == 0:
            print(f"  CB2C_INCOMING_FILE_REJ_ISS : aucune ligne a supprimer "
                  f"pour FILE_NUMBER={file_number}")
            return 0

        cursor.execute(
            "DELETE FROM CB2C_INCOMING_FILE_REJ_ISS "
            "WHERE TRIM(FILE_NUMBER) = :fn",
            {"fn": file_number}
        )
        conn.commit()

        print(f"  CB2C_INCOMING_FILE_REJ_ISS : {nb_avant} ligne(s) supprimee(s) "
              f"pour FILE_NUMBER={file_number}")
        return nb_avant

    finally:
        cursor.close()
        conn.close()
import allure
import pytest

from utils.arn_calculator import calculer_arn_complet
from utils.db_connector import execute_query
from utils.file_number_extractor import extraire_file_number


@allure.feature("Validation CB2C")
@allure.story("B. Base Oracle")
@allure.severity(allure.severity_level.BLOCKER)
@allure.title("Verification ARN dans table Oracle cible")
@allure.tag("oracle", "arn", "base-de-donnees", "verification")
@allure.description(
    "Calcule les ARNs depuis le fichier incoming, puis verifie que chacun "
    "est bien insere dans la table Oracle cible apres execution du batch."
)
def test_arn_dans_table(fichier_cas, table_verification, config, cas):
    exit_code_attendu = config["cas"][cas].get("exit_code_attendu", 1)

    # ── Cas rejet (exit 99 ou 11) : verification dans la table de rejet ──────
    if exit_code_attendu != 1:
        _verifier_table_rejet(fichier_cas, table_verification, cas, exit_code_attendu)
        return

    # ── Cas nominal (exit 1) : verification standard dans table_verification ─

    def sql_quote(value: str) -> str:
        return value.replace("'", "''")

    # ETAPE 1 : Calcul des ARNs
    with allure.step("Calcul ARN depuis fichier"):
        arns = calculer_arn_complet(fichier_cas)
        assert arns, "Aucun ARN calcule depuis le fichier"

        allure.attach(
            f"Fichier source : {fichier_cas}\n"
            f"Nombre d'ARNs  : {len(arns)}\n\n"
            + "\n".join(f"[{i + 1:02d}] {arn}" for i, arn in enumerate(arns)),
            name="ARNs calcules",
            attachment_type=allure.attachment_type.TEXT,
        )

    # ETAPE 2 : Verification presence ARN dans table_verification
    with allure.step(f"Verification dans {table_verification}"):
        erreurs   = []
        resultats = []

        for arn in arns:
            arn_sql = sql_quote(arn)
            query   = f"""
                SELECT COUNT(*) AS NB
                FROM {table_verification}
                WHERE MICROFILM_REF_NUMBER = '{arn_sql}'
            """
            rows   = execute_query(query)
            nb     = int(rows[0]["NB"]) if rows else 0
            statut = "TROUVE" if nb > 0 else "ABSENT"

            resultats.append((arn, nb, statut))
            print(f"ARN {arn} : {nb} ligne(s) dans {table_verification}")

            allure.attach(
                f"ARN            : {arn}\n"
                f"Table cible    : {table_verification}\n"
                f"Lignes trouvees: {nb}\n"
                f"Statut         : {statut}",
                name=f"Oracle - {arn}",
                attachment_type=allure.attachment_type.TEXT,
            )

            if nb == 0:
                erreurs.append(arn)

        sep = "-" * 60
        lignes_recap = [
            "=" * 60,
            "RECAPITULATIF VERIFICATION ORACLE",
            "=" * 60,
            f"{'ARN':<36}{'NB':>6}  STATUT",
            sep,
        ]
        for arn, nb, statut in resultats:
            lignes_recap.append(f"{arn:<36}{nb:>6}  {statut}")
        lignes_recap.extend(
            [
                sep,
                f"ARNs verifies : {len(arns)}",
                f"ARNs trouves  : {len(arns) - len(erreurs)}",
                f"ARNs absents  : {len(erreurs)}",
                f"Couverture    : {round((len(arns) - len(erreurs)) / len(arns) * 100, 1)}%",
                "=" * 60,
            ]
        )
        allure.attach(
            "\n".join(lignes_recap),
            name="Recapitulatif Oracle",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert not erreurs, f"{len(erreurs)} ARN(s) absent(s) de {table_verification} : {erreurs}"

    # ETAPE 3 : Lecture champs metier
    CHAMPS = ["function_code", "reversal_flag", "transaction_code"]
    CHAMPS_AFFICHAGE = {
        "function_code":    "Function Code   ",
        "reversal_flag":    "Reversal Flag   ",
        "transaction_code": "Transaction Code",
    }

    with allure.step(f"Verification champs metier dans {table_verification}"):

        lignes_table = [
            "=" * 70,
            f"TABLE : {table_verification.upper()}",
            f"CAS   : {cas}",
            "=" * 70,
            f"{'ARN':<36} {'FUNCTION_CODE':<16} {'REVERSAL_FLAG':<14} {'TRANSACTION_CODE'}",
            "-" * 70,
        ]

        for arn in arns:
            arn_sql    = sql_quote(arn)
            query_hist = f"""
                SELECT function_code,
                       reversal_flag,
                       transaction_code
                FROM   {table_verification}
                WHERE  microfilm_ref_number = '{arn_sql}'
            """
            rows_hist = execute_query(query_hist)

            if not rows_hist:
                lignes_table.append(f"{arn:<36} {'(aucune ligne trouvee)'}")
                print(f"[{table_verification}] ARN {arn} : aucune ligne trouvee")
                allure.attach(
                    f"Table          : {table_verification}\n"
                    f"ARN            : {arn}\n"
                    f"Statut         : ABSENT",
                    name=f"{table_verification} - {arn} - ABSENT",
                    attachment_type=allure.attachment_type.TEXT,
                )
            else:
                for idx, row in enumerate(rows_hist, start=1):
                    def get_field(r, field):
                        return r.get(field.upper(), r.get(field, "N/A"))

                    fc = get_field(row, "function_code")
                    rf = get_field(row, "reversal_flag")
                    tc = get_field(row, "transaction_code")

                    suffix = f"  (ligne {idx}/{len(rows_hist)})" if len(rows_hist) > 1 else ""
                    lignes_table.append(
                        f"{arn:<36} {str(fc):<16} {str(rf):<14} {str(tc):<16}{suffix}"
                    )
                    print(
                        f"[{table_verification}] ARN {arn} | "
                        f"function_code={fc} | reversal_flag={rf} | transaction_code={tc}"
                    )
                    allure.attach(
                        f"Table          : {table_verification}\n"
                        f"Cas            : {cas}\n"
                        f"ARN            : {arn}\n"
                        f"Ligne          : {idx}/{len(rows_hist)}\n"
                        f"{'─' * 40}\n"
                        + "\n".join(
                            f"{CHAMPS_AFFICHAGE[c]}: {get_field(row, c)}"
                            for c in CHAMPS
                        ),
                        name=f"{table_verification} - {arn}"
                             + (f" (l.{idx})" if len(rows_hist) > 1 else ""),
                        attachment_type=allure.attachment_type.TEXT,
                    )

        lignes_table.append("=" * 70)
        allure.attach(
            "\n".join(lignes_table),
            name=f"Recapitulatif champs metier - {table_verification}",
            attachment_type=allure.attachment_type.TEXT,
        )

    # ETAPE 4 : Synthese finale
    with allure.step("Synthese verification Oracle"):
        couverture = round((len(arns) - len(erreurs)) / len(arns) * 100, 1)
        allure.attach(
            f"{'=' * 45}\n"
            "SYNTHESE ORACLE\n"
            f"{'=' * 45}\n"
            f"Cas            : {cas}\n"
            f"Table cible    : {table_verification}\n"
            f"ARNs verifies  : {len(arns)}\n"
            f"ARNs trouves   : {len(arns) - len(erreurs)}\n"
            f"ARNs absents   : {len(erreurs)}\n"
            f"Couverture     : {couverture}%\n"
            "Statut global  : PASSED\n"
            f"{'=' * 45}",
            name="Synthese Oracle",
            attachment_type=allure.attachment_type.TEXT,
        )
        print("Verification Oracle OK")


# ─────────────────────────────────────────────────────────────────────────────
# Verification table rejet — cas3 (exit 99 / 11)
# Cle de recherche : FILE_NUMBER (extrait du header du fichier incoming)
# ─────────────────────────────────────────────────────────────────────────────
def _verifier_table_rejet(fichier_cas, table_verification, cas, exit_code_attendu):
    """Verifie la presence du fichier dans CB2C_INCOMING_FILE_REJ_ISS par FILE_NUMBER."""

    # ETAPE 1 : extraction FILE_NUMBER
    with allure.step("Extraction FILE_NUMBER depuis fichier"):
        file_number = extraire_file_number(fichier_cas)
        allure.attach(
            f"Fichier source : {fichier_cas}\n"
            f"FILE_NUMBER    : {file_number}\n"
            f"Exit attendu   : {exit_code_attendu}",
            name="FILE_NUMBER extrait",
            attachment_type=allure.attachment_type.TEXT,
        )

    # ETAPE 2 : verification presence dans table rejet
    with allure.step(f"Verification dans {table_verification}"):
        rows   = execute_query(
            f"SELECT COUNT(*) AS NB FROM {table_verification} "
             f"WHERE TRIM(FILE_NUMBER) = '{file_number.strip()}'"
        )
        nb     = int(rows[0]["NB"]) if rows else 0
        statut = "TROUVE" if nb > 0 else "ABSENT"

        print(f"FILE_NUMBER {file_number} : {nb} ligne(s) dans {table_verification}")
        allure.attach(
            f"FILE_NUMBER    : {file_number}\n"
            f"Table rejet    : {table_verification}\n"
            f"Lignes trouvees: {nb}\n"
            f"Statut         : {statut}",
            name="Verification table rejet",
            attachment_type=allure.attachment_type.TEXT,
        )
        assert nb > 0, f"FILE_NUMBER {file_number} absent de {table_verification}"

    # ETAPE 3 : lecture champs metier
    CHAMPS = ["reason_code", "reject_level", "file_number", "date_create"]
    CHAMPS_AFFICHAGE = {
        "reason_code"  : "Reason Code  ",
        "reject_level" : "Reject Level ",
        "file_number"  : "File Number  ",
        "date_create"  : "Date Create  ",
    }

    with allure.step(f"Verification champs metier dans {table_verification}"):
        def get_field(r, field):
            return r.get(field.upper(), r.get(field, "N/A"))

        rows_rej = execute_query(
            f"SELECT reason_code, reject_level, file_number, date_create "
            f"FROM {table_verification} "
              f"WHERE TRIM(FILE_NUMBER) = '{file_number.strip()}' "
        )

        lignes_table = [
            "=" * 80,
            f"TABLE : {table_verification.upper()}",
            f"CAS   : {cas}",
            f"FILE_NUMBER : {file_number}",
            "=" * 80,
            f"{'REASON_CODE':<14} {'REJECT_LEVEL':<14} {'FILE_NUMBER':<14} {'DATE_CREATE'}",
            "-" * 80,
        ]

        for idx, row in enumerate(rows_rej, start=1):
            rc  = get_field(row, "reason_code")
            rl  = get_field(row, "reject_level")
            fn  = get_field(row, "file_number")
            dc  = get_field(row, "date_create")

            suffix = f"  (ligne {idx}/{len(rows_rej)})" if len(rows_rej) > 1 else ""
            lignes_table.append(
                f"{str(rc):<14} {str(rl):<14} {str(fn):<14} {str(dc)}{suffix}"
            )
            print(
                f"[{table_verification}] FILE_NUMBER={file_number} | "
                f"reason_code={rc} | reject_level={rl} | "
                f"file_number={fn} | date_create={dc}"
            )
            allure.attach(
                f"Table          : {table_verification}\n"
                f"Cas            : {cas}\n"
                f"FILE_NUMBER    : {file_number}\n"
                f"Ligne          : {idx}/{len(rows_rej)}\n"
                f"{'─' * 40}\n"
                + "\n".join(
                    f"{CHAMPS_AFFICHAGE[c]}: {get_field(row, c)}"
                    for c in CHAMPS
                ),
                name=f"{table_verification} - FILE_NUMBER {file_number}"
                     + (f" (l.{idx})" if len(rows_rej) > 1 else ""),
                attachment_type=allure.attachment_type.TEXT,
            )

        lignes_table.append("=" * 80)
        allure.attach(
            "\n".join(lignes_table),
            name=f"Recapitulatif champs metier - {table_verification}",
            attachment_type=allure.attachment_type.TEXT,
        )

    # ETAPE 4 : synthese
    with allure.step("Synthese verification table rejet"):
        allure.attach(
            f"{'=' * 45}\n"
            "SYNTHESE REJET TECHNIQUE\n"
            f"{'=' * 45}\n"
            f"Cas            : {cas}\n"
            f"Table rejet    : {table_verification}\n"
            f"FILE_NUMBER    : {file_number}\n"
            f"Exit attendu   : {exit_code_attendu}\n"
            f"Lignes trouvees: {nb}\n"
            "Statut global  : PASSED\n"
            f"{'=' * 45}",
            name="Synthese Rejet Technique",
            attachment_type=allure.attachment_type.TEXT,
        )
        print("Verification table rejet OK")
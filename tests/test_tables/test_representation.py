import pytest
import allure
import yaml
from pathlib import Path

from utils.arn_calculator import calculer_arn_complet
from utils.db_connector import execute_query


# ─── Chargement mapping case_reason_code depuis YAML     ─────

_MAPPING_PATH = Path(__file__).parent.parent.parent / "config" / "case_reason_code_mapping.yaml"

with _MAPPING_PATH.open(encoding="utf-8") as _f:
    CASE_REASON_CODE_MAPPING: dict = yaml.safe_load(_f).get("CASE_REASON_CODE_MAPPING", {})


# ─── Requetes SQL             

SQL_CASE_TABLE = """
    SELECT *
    FROM case_table
    WHERE microfilm_ref_number = '{microfilm}'
    AND criteria_7 = '1'
"""

SQL_EVENT_TABLE = """
    SELECT *
    FROM event_table
    WHERE case_reference = '{case_reference}'
    AND case_reason_code = '{case_reason_code}'
"""

SQL_LINK_EVENT_SCENARIO = """
    SELECT *
    FROM link_event_scenario
    WHERE event_reference = '{event_reference}'
"""

SQL_CASE_REASON = """
    SELECT *
    FROM case_reason
    WHERE bank_code = '{bank_code}'
"""

SQL_ACTION_TABLE = """
    SELECT *
    FROM action_table
    WHERE action_code = '{action_code}'
    AND bank_code = '{bank_code}'
"""

SEP  = "-" * 60
SEP2 = "=" * 60


# ─── Test principal         ───────────────────

@allure.feature("Validation CB2C")
@allure.story("C. Representation Transaction")
@allure.severity(allure.severity_level.CRITICAL)
@allure.title("Verification Representation Transaction — cas2")
def test_verification_representation(fichier_cas, config, cas):

    # ── Calcul ARNs         ───────────────────
    with allure.step("Calcul ARNs depuis fichier incoming"):
        arns = calculer_arn_complet(fichier_cas)
        assert arns, "Aucun ARN calcule depuis le fichier"
        allure.attach(
            f"Fichier source : {fichier_cas}\n"
            f"Nombre d'ARNs  : {len(arns)}\n\n"
            + "\n".join(f"[{i+1:02d}] {arn}" for i, arn in enumerate(arns)),
            name="ARNs calcules",
            attachment_type=allure.attachment_type.TEXT,
        )

    erreurs = []

    for arn in arns:

        print(f"\n{SEP2}")
        print(f"  VERIFICATION REPRESENTATION — ARN : {arn}")
        print(f"{SEP2}")

        # ── STEP 1 : case_table         ───────
        with allure.step(f"[{arn}] STEP 1 — Dossier litige unitaire (case_table)"):
            rows_case = execute_query(SQL_CASE_TABLE.format(microfilm=arn))
            if not rows_case:
                msg = (f"Aucun dossier litige unitaire dans case_table "
                       f"pour microfilm_ref_number='{arn}' et criteria_7='1'")
                allure.attach(msg, name=f"[{arn}] case_table ABSENT",
                              attachment_type=allure.attachment_type.TEXT)
                erreurs.append(f"[{arn}] case_table : introuvable")
                print(f"  [STEP 1] case_table         : ABSENT")
                continue

            case_row       = rows_case[0]
            case_reference = case_row.get("CASE_REFERENCE") or case_row.get("case_reference")
            bank_code      = case_row.get("BANK_CODE")      or case_row.get("bank_code")
            allure.attach(str(case_row), name=f"[{arn}] case_table OK",
                          attachment_type=allure.attachment_type.TEXT)

            print(f"  [STEP 1] case_table")
            print(f"  {SEP}")
            print(f"    microfilm_ref_number : {arn}")
            print(f"    criteria_7           : 1  (dossier litige unitaire / ARN sequence)")
            print(f"    >> case_reference    : {case_reference}")
            print(f"    >> bank_code         : {bank_code}")
            print(f"  {SEP}")

        # ── STEP 2 : event_table         ──────
        with allure.step(f"[{arn}] STEP 2 — Evenement Reception Representation (event_table)"):
            case_reason_code = "0004"
            libelle          = CASE_REASON_CODE_MAPPING.get(case_reason_code, case_reason_code)
            rows_event = execute_query(SQL_EVENT_TABLE.format(
                case_reference   =case_reference,
                case_reason_code =case_reason_code,
            ))
            if not rows_event:
                msg = (f"Aucun evenement '{libelle}' (code {case_reason_code}) "
                       f"dans event_table pour case_reference='{case_reference}'")
                allure.attach(msg, name=f"[{arn}] event_table ABSENT",
                              attachment_type=allure.attachment_type.TEXT)
                erreurs.append(f"[{arn}] event_table : introuvable (code {case_reason_code})")
                print(f"  [STEP 2] event_table        : ABSENT")
                continue

            event_row       = rows_event[0]
            event_reference = event_row.get("EVENT_REFERENCE") or event_row.get("event_reference")
            allure.attach(str(event_row), name=f"[{arn}] event_table OK",
                          attachment_type=allure.attachment_type.TEXT)

            print(f"  [STEP 2] event_table")
            print(f"  {SEP}")
            print(f"    case_reference   : {case_reference}")
            print(f"    case_reason_code : {case_reason_code}  ({libelle})")
            print(f"    >> event_reference : {event_reference}")
            print(f"  {SEP}")

        # ── STEP 3 : link_event_scenario     ──────────────────
        with allure.step(f"[{arn}] STEP 3 — Lien evenement/scenario (link_event_scenario)"):
            rows_link = execute_query(SQL_LINK_EVENT_SCENARIO.format(
                event_reference=event_reference,
            ))
            if not rows_link:
                msg = (f"Aucun lien dans link_event_scenario "
                       f"pour event_reference='{event_reference}'")
                allure.attach(msg, name=f"[{arn}] link_event_scenario ABSENT",
                              attachment_type=allure.attachment_type.TEXT)
                erreurs.append(f"[{arn}] link_event_scenario : introuvable")
                print(f"  [STEP 3] link_event_scenario: ABSENT")
                continue

            link_row    = rows_link[0]
            action_code = link_row.get("ACTION_CODE") or link_row.get("action_code")
            allure.attach(str(rows_link), name=f"[{arn}] link_event_scenario OK",
                          attachment_type=allure.attachment_type.TEXT)

            print(f"  [STEP 3] link_event_scenario")
            print(f"  {SEP}")
            print(f"    event_reference  : {event_reference}")
            print(f"    >> action_code   : {action_code}")
            print(f"  {SEP}")

        # ── STEP 4 : case_reason         ──────
        with allure.step(f"[{arn}] STEP 4 — Parametrage evenement (case_reason)"):
            rows_reason = execute_query(SQL_CASE_REASON.format(bank_code=bank_code))
            if not rows_reason:
                msg = (f"Aucun parametrage dans case_reason "
                       f"pour bank_code='{bank_code}'")
                allure.attach(msg, name=f"[{arn}] case_reason ABSENT",
                              attachment_type=allure.attachment_type.TEXT)
                erreurs.append(f"[{arn}] case_reason : introuvable")
                print(f"  [STEP 4] case_reason        : ABSENT")
                continue

            allure.attach(str(rows_reason), name=f"[{arn}] case_reason OK",
                          attachment_type=allure.attachment_type.TEXT)

            print(f"  [STEP 4] case_reason  (parametrage evenements)")
            print(f"  {SEP}")
            print(f"    bank_code        : {bank_code}")
            print(f"    >> lignes        : {len(rows_reason)}")
            print(f"  {SEP}")

        # ── STEP 5 : action_table         ─────
        with allure.step(f"[{arn}] STEP 5 — Parametrage action (action_table)"):
            rows_action = execute_query(SQL_ACTION_TABLE.format(
                action_code=action_code,
                bank_code  =bank_code,
            ))
            if not rows_action:
                msg = (f"Aucun parametrage dans action_table "
                       f"pour action_code='{action_code}' et bank_code='{bank_code}'")
                allure.attach(msg, name=f"[{arn}] action_table ABSENT",
                              attachment_type=allure.attachment_type.TEXT)
                erreurs.append(f"[{arn}] action_table : introuvable")
                print(f"  [STEP 5] action_table       : ABSENT")
                continue

            allure.attach(str(rows_action), name=f"[{arn}] action_table OK",
                          attachment_type=allure.attachment_type.TEXT)

            print(f"  [STEP 5] action_table  (parametrage actions)")
            print(f"  {SEP}")
            print(f"    action_code      : {action_code}")
            print(f"    bank_code        : {bank_code}")
            print(f"    >> lignes        : {len(rows_action)}")
            print(f"  {SEP}")

        print(f"\n  RESULTAT ARN {arn} : TOUTES LES VERIFICATIONS PASSEES")

    # ── Recapitulatif         ─────────────────
    print(f"\n{SEP2}")
    print(f"  RECAPITULATIF VERIFICATION REPRESENTATION")
    print(f"{SEP2}")
    print(f"  ARNs verifies  : {len(arns)}")
    print(f"  ARNs OK        : {len(arns) - len(erreurs)}")
    print(f"  ARNs en erreur : {len(erreurs)}")
    if erreurs:
        print(f"  {SEP}")
        print(f"  ERREURS :")
        for e in erreurs:
            print(f"    - {e}")
    print(f"{SEP2}")

    allure.attach(
        "\n".join([
            SEP2,
            "RECAPITULATIF VERIFICATION REPRESENTATION",
            SEP2,
            f"ARNs verifies  : {len(arns)}",
            f"ARNs OK        : {len(arns) - len(erreurs)}",
            f"ARNs en erreur : {len(erreurs)}",
        ] + ([SEP, "ERREURS :"] + [f"  - {e}" for e in erreurs] if erreurs else []) + [SEP2]),
        name="Recapitulatif Representation",
        attachment_type=allure.attachment_type.TEXT,
    )

    assert not erreurs, (
        f"{len(erreurs)} verification(s) en echec :\n" + "\n".join(erreurs)
    )

    print("\n[ETAPE 5] Verification Representation Transaction : OK")
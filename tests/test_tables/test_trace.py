import allure
import subprocess


def get_trace(config):
    proc = subprocess.Popen(
        ["bash", "-c", f"cat {config['cb2c']['trace_path']}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, _ = proc.communicate()
    return stdout.decode('utf-8', errors='replace')


def extraire_compteurs(trace):
    processed = succeeded = rejected = 0
    for ligne in trace.splitlines():
        if "Tx Processed" in ligne:
            processed = int(ligne.split(":")[-1].strip().split("]")[0].strip())
        elif "Tx Succeeded" in ligne:
            succeeded = int(ligne.split(":")[-1].strip().split("]")[0].strip())
        elif "Tx Rejected" in ligne:
            rejected  = int(ligne.split(":")[-1].strip().split("]")[0].strip())
    return processed, succeeded, rejected


def _barre_progression(valeur, total, largeur=20):
    if total == 0:
        return "░" * largeur
    rempli = round((valeur / total) * largeur)
    return "█" * rempli + "░" * (largeur - rempli)


@allure.feature("Validation CB2C")
@allure.story("D. Reporting")
@allure.severity(allure.severity_level.CRITICAL)
@allure.title("Vérification compteurs trace batch PowerCARD")
@allure.tag("trace", "reporting", "compteurs", "powercard")
@allure.description(
    "Lit la trace batch PowerCARD cb2c_load_incoming, extrait les compteurs "
    "Tx Processed / Succeeded / Rejected et valide leur cohérence selon le cas de test."
)
def test_trace_ok(config, cas):

    with allure.step("lecture fichier trace"):
        trace = get_trace(config)
        assert trace, "fichier trace vide ou introuvable"
        nb_lignes = len(trace.splitlines())
        allure.attach(
            trace,
            name="trace batch complete",
            attachment_type=allure.attachment_type.TEXT
        )
        allure.attach(
            f"Fichier  : {config['cb2c']['trace_path']}\n"
            f"Lignes   : {nb_lignes}\n"
            f"Statut   : PRESENT ",
            name="meta trace",
            attachment_type=allure.attachment_type.TEXT
        )

    with allure.step("extraction compteurs"):
        processed, succeeded, rejected = extraire_compteurs(trace)
        print(f"\nTx Processed : {processed}")
        print(f"Tx Succeeded : {succeeded}")
        print(f"Tx Rejected  : {rejected}")

        taux_succes  = round((succeeded / processed * 100) if processed > 0 else 0, 2)
        taux_rejet   = round((rejected  / processed * 100) if processed > 0 else 0, 2)
        barre_ok     = _barre_progression(succeeded, processed)
        barre_rej    = _barre_progression(rejected,  processed)

        # Tableau de compteurs lisible
        allure.attach(
            f"{'='*45}\n"
            f"  COMPTEURS BATCH\n"
            f"{'='*45}\n"
            f"  Tx Processed : {processed:>6}\n"
            f"  Tx Succeeded : {succeeded:>6}  [{barre_ok}] {taux_succes}%\n"
            f"  Tx Rejected  : {rejected:>6}  [{barre_rej}] {taux_rejet}%\n"
            f"{'='*45}",
            name="compteurs batch",
            attachment_type=allure.attachment_type.TEXT
        )

    with allure.step("validation coherence compteurs"):
        # processed doit etre egal a succeeded + rejected
        total_calcule = succeeded + rejected
        coherence_ok  = (total_calcule == processed)
        allure.attach(
            f"Processed attendu   : {processed}\n"
            f"Succeeded + Rejected: {total_calcule}\n"
            f"Coherence           : {'OK ' if coherence_ok else 'ECART '}",
            name="coherence compteurs",
            attachment_type=allure.attachment_type.TEXT
        )
        if not coherence_ok:
            allure.attach(
                f"AVERTISSEMENT : processed ({processed}) != succeeded+rejected ({total_calcule})\n"
                f"Ecart : {abs(processed - total_calcule)} transaction(s)",
                name="alerte coherence",
                attachment_type=allure.attachment_type.TEXT
            )

    with allure.step("verification compteurs selon le cas"):
        exit_code_attendu = config["cas"][cas].get("exit_code_attendu", 1)
        assert processed > 0, "aucune transaction traitee — trace batch incomplète ou batch non lancé"

        if exit_code_attendu == 1:
            assert succeeded > 0,  "aucune transaction reussie — cas nominal attend succeeded > 0"
            assert rejected  == 0, f"transactions rejetees detectees ({rejected}) — cas nominal attend 0 rejet"
            statut_trace = "cas nominal (succes)"
        else:
            assert rejected  > 0,  "aucun rejet detecte — cas rejet attend rejected > 0"
            assert succeeded == 0, f"transactions reussies inattendues ({succeeded}) — cas rejet attend 0 succes"
            statut_trace = "cas rejet (erreur attendue)"

        print(f"trace ok — {statut_trace}")

    with allure.step("synthese trace"):
        allure.attach(
            f"{'='*45}\n"
            f"  SYNTHESE TRACE\n"
            f"{'='*45}\n"
            f"  Cas            : {cas}\n"
            f"  Type           : {statut_trace}\n"
            f"  Exit attendu   : {exit_code_attendu}\n"
            f"{'-'*45}\n"
            f"  Tx Processed   : {processed}\n"
            f"  Tx Succeeded   : {succeeded}  ({taux_succes}%)\n"
            f"  Tx Rejected    : {rejected}  ({taux_rejet}%)\n"
            f"{'-'*45}\n"
            f"  Coherence      : {'OK ' if coherence_ok else 'ECART '}\n"
            f"  Statut final   : PASSED \n"
            f"{'='*45}",
            name="synthese trace",
            attachment_type=allure.attachment_type.TEXT
        )
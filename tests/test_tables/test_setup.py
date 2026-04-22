import subprocess
import shutil
import os
import time
import allure
from utils.arn_calculator import calculer_arn_complet
from utils.clean_db import (
    clean_par_arn,
    clean_tables_fichiers,
    clean_par_file_number,
    extraire_file_number,
)


def lancer_batch(config):
    proc = subprocess.Popen(
        ["bash", "-c",
         f"source /pwrcard/home/.pcard_profile && "
         f"cd /pwrcard/home/usr/data && "
         f"sh {config['cb2c']['script']} {config['cb2c']['trace_file']}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = proc.communicate()
    proc.stdout_text = stdout.decode('utf-8', errors='replace')
    proc.stderr_text = stderr.decode('utf-8', errors='replace')
    return proc


@allure.feature("Validation CB2C")
@allure.story("A. Incoming")
@allure.severity(allure.severity_level.CRITICAL)
@allure.title("Verification existence fichier incoming")
@allure.tag("setup", "fichier", "incoming")
@allure.description("Verifie que le fichier incoming existe avant de lancer le batch")
def test_1_fichier_existe(fichier_cas):
    with allure.step("verification existence fichier incoming"):
        assert os.path.exists(fichier_cas), f"fichier pas trouve : {fichier_cas}"
        allure.attach(
            f"Chemin  : {fichier_cas}\n"
            f"Nom     : {os.path.basename(fichier_cas)}\n"
            f"Taille  : {os.path.getsize(fichier_cas)} octets\n"
            f"Statut  : PRESENT OK",
            name="details fichier",
            attachment_type=allure.attachment_type.TEXT
        )


@allure.feature("Validation CB2C")
@allure.story("A. Incoming")
@allure.severity(allure.severity_level.CRITICAL)
@allure.title("Nettoyage Oracle avant test")
@allure.tag("setup", "clean", "oracle")
@allure.description(
    "Cas 1 : clean full — supprime ARNs + tables fichiers.\n"
    "Cas 2 : clean tables_only — tables fichiers uniquement, ARNs conserves.\n"
    "Cas 3 : clean rejet — supprime ARNs + tables fichiers + CB2C_INCOMING_FILE_REJ_ISS par FILE_NUMBER."
)
def test_2_clean(fichier_cas, config, cas):
    strategy    = config["cas"][cas].get("clean_strategy", "full")
    arns        = []
    file_number = "N/A"

    allure.attach(
        f"Cas         : {cas}\n"
        f"Strategie   : {strategy}\n"
        f"Fichier     : {os.path.basename(fichier_cas)}",
        name="strategie clean",
        attachment_type=allure.attachment_type.TEXT
    )

    # ── cas1 (full) + cas3 (rejet) : clean par ARN   clean çndependemament des cas ───────────────────────────
    if strategy in ("full", "rejet"):
        with allure.step("calcul ARN depuis fichier"):
            arns = calculer_arn_complet(fichier_cas)
            assert len(arns) > 0, "aucun arn calcule"
            allure.attach(
                f"Nombre d'ARNs : {len(arns)}\n\n"
                + "\n".join(f"  - {a}" for a in arns),
                name="arns a nettoyer",
                attachment_type=allure.attachment_type.TEXT
            )
        with allure.step(f"nettoyage par ARN ({len(arns)} ARN(s))"):
            for arn in arns:
                clean_par_arn(arn)

    # ── tous les cas : clean tables fichiers ─────────────────────────────────
    with allure.step(f"nettoyage tables fichiers ({strategy})"):
        clean_tables_fichiers(strategy=strategy)

    # ── cas3 (rejet) uniquement : clean CB2C_INCOMING_FILE_REJ_ISS ───────────
    if strategy == "rejet":
        with allure.step("nettoyage CB2C_INCOMING_FILE_REJ_ISS par file_number"):
            file_number = extraire_file_number(fichier_cas)
            nb_suppr    = clean_par_file_number(file_number)
            allure.attach(
                f"FILE_NUMBER       : {file_number}\n"
                f"Lignes supprimees : {nb_suppr}",
                name="clean rejet file_number",
                attachment_type=allure.attachment_type.TEXT
            )

    # ── resume ────────────────────────────────────────────────────────────────
    allure.attach(
        f"Statut clean      : OK\n"
        f"Strategie         : {strategy}\n"
        f"ARNs traites      : {len(arns) if strategy in ('full', 'rejet') else 'N/A — ARNs conserves'}\n"
        f"File number       : {file_number}",
        name="resultat clean",
        attachment_type=allure.attachment_type.TEXT
    )


@allure.feature("Validation CB2C")
@allure.story("A. Incoming")
@allure.severity(allure.severity_level.NORMAL)
@allure.title("Copie fichier vers repertoire batch")
@allure.tag("setup", "copie", "incoming")
@allure.description(
    "Copie le fichier incoming vers la destination batch definie dans la config. "
    "La destination est un chemin fichier complet (pas un dossier)."
)
def test_3_copie(fichier_cas, config):
    destination = config["cb2c"]["destination"]

    with allure.step("copie fichier vers destination"):
        shutil.copy(fichier_cas, destination)

        assert os.path.exists(destination), \
            f"copie echouee — fichier absent : {destination}"
        assert os.path.getsize(destination) == os.path.getsize(fichier_cas), \
            f"integrite KO — taille source={os.path.getsize(fichier_cas)} " \
            f"taille dest={os.path.getsize(destination)}"

        allure.attach(
            f"Source          : {fichier_cas}\n"
            f"Destination     : {destination}\n"
            f"Taille source   : {os.path.getsize(fichier_cas)} octets\n"
            f"Taille dest     : {os.path.getsize(destination)} octets\n"
            f"Integrite       : OK\n"
            f"Statut          : COPIE OK",
            name="details copie",
            attachment_type=allure.attachment_type.TEXT
        )


@allure.feature("Validation CB2C")
@allure.story("A. Incoming")
@allure.severity(allure.severity_level.BLOCKER)
@allure.title("Execution batch cb2c_load_incoming")
@allure.tag("batch", "execution", "powercard")
@allure.description(
    "Cas 1 (bon a payer) : exit 1 attendu.\n"
    "Cas 2 (representation) : exit 1 attendu.\n"
    "Cas 3 (rejet technique) : exit 99 attendu (config), 11 aussi accepte."
)
def test_4_batch(config, cas):
    cas_config = config["cas"][cas]

    exit_code_config    = cas_config.get("exit_code_attendu", 1)
    exit_codes_attendus = (
        [11, 99] if exit_code_config == 99
        else [exit_code_config]
    )

    with allure.step("suppression trace precedente"):
        subprocess.Popen(
            ["bash", "-c", f"rm -f {config['cb2c']['trace_path']}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        ).communicate()

    with allure.step("lancement batch"):
        t0     = time.time()
        result = lancer_batch(config)
        duree  = round(time.time() - t0, 2)

        print(f"exit code : {result.returncode}  |  duree : {duree}s")
        if result.stdout_text:
            print(result.stdout_text[:1000])

        statut_exit = "OK" if result.returncode in exit_codes_attendus else "INATTENDU"
        allure.attach(
            f"{'='*50}\n"
            f"  RESULTAT BATCH\n"
            f"{'='*50}\n"
            f"Script              : {config['cb2c']['script']}\n"
            f"Fichier trace       : {config['cb2c']['trace_file']}\n"
            f"Duree               : {duree}s\n"
            f"Exit code reel      : {result.returncode}\n"
            f"Exit codes attendus : {exit_codes_attendus}\n"
            f"Statut              : {statut_exit}\n"
            f"{'='*50}\n\n"
            f"--- STDERR ---\n{result.stderr_text or '(vide)'}\n\n"
            f"--- STDOUT ---\n{result.stdout_text[:2000] if result.stdout_text else '(vide)'}",
            name="resultat batch complet",
            attachment_type=allure.attachment_type.TEXT
        )

    with allure.step("verification exit code"):
        assert result.returncode in exit_codes_attendus, (
            f"exit code inattendu : obtenu={result.returncode}, "
            f"attendus={exit_codes_attendus} (cas={cas})"
        )

    with allure.step("synthese batch"):
        allure.attach(
            f"Batch               : cb2c_load_incoming.sh\n"
            f"Cas                 : {cas} — {cas_config['nom']}\n"
            f"Exit code reel      : {result.returncode}\n"
            f"Exit codes attendus : {exit_codes_attendus}\n"
            f"Duree execution     : {duree}s\n"
            f"Statut final        : PASSED",
            name="synthese batch",
            attachment_type=allure.attachment_type.TEXT
        )
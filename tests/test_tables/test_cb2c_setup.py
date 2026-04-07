import subprocess
import shutil
import os
import yaml
from utils.arn_calculator import calculer_arn_complet
from utils.clean_db import clean_par_arn, clean_tables_fichiers


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def test_fichier_incoming_existe():
    cfg = load_config()
    source = cfg["paths"]["cb2c"]["source"]
    print(f"\nrecherche du fichier : {source}")
    assert os.path.exists(source), f"fichier pas trouve : {source}"
    print("fichier ok")


def test_copie_fichier_incoming():
    cfg = load_config()
    source = cfg["paths"]["cb2c"]["source"]
    destination = cfg["paths"]["cb2c"]["destination"]
    print(f"\ncopie de {source} vers {destination}")
    shutil.copy(source, destination)
    assert os.path.exists(destination), "copie echouee"
    print("copie ok")


def test_clean_avant_batch():
    cfg = load_config()
    fichier = cfg["paths"]["cb2c"]["source"]
    arns = calculer_arn_complet(fichier)
    print(f"\narns a nettoyer : {arns}")
    assert len(arns) > 0, "aucun arn calcule"
    for arn in arns:
        clean_par_arn(arn)
    clean_tables_fichiers()


def test_batch_cb2c_exit_code():
    cfg = load_config()
    print("\nlancement du batch cb2c...")
    result = subprocess.run(
        ["docker", "exec", "-u", "pwrcard", "CAPS-unix-3.5.4-RC2",
         "bash", "-c",
         f"source /pwrcard/home/.pcard_profile && cd /pwrcard/home/usr/data && sh {cfg['paths']['cb2c']['script']} {cfg['paths']['cb2c']['trace']}"],
        capture_output=True,
        text=True
    )
    print(f"exit code : {result.returncode}")
    if result.stderr:
        print(f"stderr : {result.stderr[:200]}")
    assert result.returncode not in [11, 99], \
        f"batch echoue : exit {result.returncode}"
    assert result.returncode == 1
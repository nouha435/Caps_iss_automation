import subprocess
import sys
import argparse
import time
import yaml
import os


TEST_FILES = [
    ("tests/test_tables/test_setup.py",  "ETAPE 1", "Clean + Copie + Batch"),
    ("tests/test_tables/test_trace.py",  "ETAPE 2", "Verification Trace"),
    ("tests/test_tables/test_oracle.py", "ETAPE 3", "Verification Oracle"),
]

ETAPE_INSERT = ("tests/test_tables/test_insert.py",         "ETAPE 4", "Insertion case_table")
ETAPE_REPR   = ("tests/test_tables/test_representation.py", "ETAPE 5", "Verification Representation Transaction")


def load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)


def run_tests(path, cas):
    proc = subprocess.Popen(
        ["python3", "-m", "pytest", path,
         "-v", "-s", "--no-header", "--tb=short",
         f"--cas={cas}", "-p", "no:html",
         "--alluredir=reports/allure"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, _ = proc.communicate()
    print(stdout.decode('utf-8', errors='replace'))
    return proc.returncode


def run_etape(path, etape, description, cas, resultats):
    """Execute une etape et ajoute le resultat dans la liste"""
    print(f"\n{etape} : {description}")
    debut  = time.time()
    code   = run_tests(path, cas)
    duree  = f"{round(time.time() - debut, 2)}s"
    statut = "PASSED" if code == 0 else "FAILED"
    resultats.append({
        "etape"      : etape,
        "description": description,
        "statut"     : statut,
        "duree"      : duree
    })


def generer_environment(cas, cfg):
    """genere environment.properties pour Allure — visible dans Overview"""
    allure_dir = cfg["rapport"]["allure_dir"]
    os.makedirs(allure_dir, exist_ok=True)
    chemin = f"{allure_dir}/environment.properties"

    mode = "a" if os.path.exists(chemin) else "w"
    with open(chemin, mode) as f:
        f.write(f"\n# {cfg['cas'][cas]['nom']}\n")
        f.write(f"{cas}.Nom={cfg['cas'][cas]['nom']}\n")
        f.write(f"{cas}.Fichier={os.path.basename(cfg['cas'][cas]['fichier'])}\n")
        f.write(f"{cas}.Table={cfg['cas'][cas]['table_verification']}\n")
        f.write(f"Environnement=CAPS-unix-3.5.4-RC2\n")
        f.write(f"Base=Oracle 19c\n")
        f.write(f"Batch=cb2c_load_incoming.sh\n")
        f.write(f"Python=3.6.8\n")
        f.write(f"pytest=7.0.1\n")


def executer_cas(cas, cfg, resultats):
    """Execute les etapes communes pour un cas donne"""
    nom = cfg["cas"][cas]["nom"]

    print(f"\n{'='*60}")
    print(f"  {nom}")
    print(f"{'='*60}")

    generer_environment(cas, cfg)

    for path, etape, description in TEST_FILES:
        run_etape(path, etape, description, cas, resultats)


def main():
    parser = argparse.ArgumentParser(description="Automatisation CB2C")
    parser.add_argument("--cas", required=True, help="cas a executer : cas1, cas2, cas3, all...")
    args = parser.parse_args()

    cas = args.cas
    cfg = load_config()

    debut_global = time.time()
    resultats    = []

    if cas == "cas1":
        # ── cas1 : ETAPE 1 → 2 → 3 ──────────────────────────────────────────
        executer_cas("cas1", cfg, resultats)

    elif cas == "cas2":
        # ── cas2 : INSERT (cas1) → ETAPE 1 → 2 → 3 → ETAPE 5  boucle a ajouter pour les cas ───────────────
        print(f"\n{'='*60}")
        print(f"  PRE-REQUIS : Insertion case_table (issue de cas1)")
        print(f"{'='*60}")
        path, etape, description = ETAPE_INSERT
        run_etape(path, etape, description, "cas1", resultats)

        executer_cas("cas2", cfg, resultats)

        path, etape, description = ETAPE_REPR
        run_etape(path, etape, description, "cas2", resultats)

    elif cas == "cas3":
        # ── cas3 : ETAPE 1 (exit 99/11) → ETAPE 2 → ETAPE 3 (table rejet) ──
        executer_cas("cas3", cfg, resultats)

    elif cas == "all":
        # ── all : cas1 → INSERT → cas2 → REPR → cas3 ─────────────────────────
        executer_cas("cas1", cfg, resultats)

        print(f"\n{'='*60}")
        print(f"  ETAPE INTER-CAS : Insertion case_table (apres cas1, avant cas2)")
        print(f"{'='*60}")
        path, etape, description = ETAPE_INSERT
        run_etape(path, etape, description, "cas1", resultats)

        executer_cas("cas2", cfg, resultats)

        path, etape, description = ETAPE_REPR
        run_etape(path, etape, description, "cas2", resultats)

        # ── cas3 apres cas2 ───────────────────────────────────────────────────
        executer_cas("cas3", cfg, resultats)

    else:
        if cas not in cfg["cas"]:
            print(f"cas inconnu : {cas}")
            print(f"cas disponibles : {list(cfg['cas'].keys())} ou 'all'")
            sys.exit(1)
        executer_cas(cas, cfg, resultats)

    duree_totale = round(time.time() - debut_global, 2)

    print(f"\n{'='*60}")
    print("  RAPPORT FINAL")
    print(f"{'='*60}")
    for r in resultats:
        icone = "PASSED" if r["statut"] == "PASSED" else "FAILED"
        print(f"  {icone} — {r['etape']} : {r['description']} ({r['duree']})")
    print(f"\n  Duree totale : {duree_totale}s")
    print(f"  Cas          : {cas}")

    if any(r["statut"] == "FAILED" for r in resultats):
        sys.exit(1)


if __name__ == "__main__":
    main()
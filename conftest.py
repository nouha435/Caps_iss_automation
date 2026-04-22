import pytest
import yaml
import allure
from utils.oracle_driver import driver as cx_Oracle


# ─── chargement config     

def _load_config():
    with open("config/config.yaml") as f:
        return yaml.safe_load(f)




@pytest.fixture(scope="session")
def config():
    return _load_config()


@pytest.fixture(scope="session")
def db_connection(config):
    """Connexion Oracle partagee pour toute la session de test"""
    dsn = cx_Oracle.makedsn(
        config["oracle"]["host"],
        config["oracle"]["port"],
        service_name=config["oracle"]["service_name"]
    )
    conn = cx_Oracle.connect(
        user=config["oracle"]["user"],
        password=config["oracle"]["password"],
        dsn=dsn
    )
    yield conn
    conn.close()


# ─── parametrize dynamique sur TOUS les cas du yaml    ──────

def pytest_generate_tests(metafunc):
    """
    Si un test demande la fixture 'cas', on l'alimente dynamiquement
    avec tous les cas trouvés dans config.yaml.
    On peut restreindre via : pytest --cas cas1,cas2
    """
    if "cas" not in metafunc.fixturenames:
        return

    cfg     = _load_config()
    tous    = list(cfg["cas"].keys())                        # ["cas1", "cas2", ...]
    option  = metafunc.config.getoption("--cas", default=None)

    if option:
        # --cas cas1        → [cas1]
        # --cas cas1,cas2   → [cas1, cas2]
        selectionnes = [c.strip() for c in option.split(",") if c.strip() in tous]
    else:
        selectionnes = tous                                  # tous les cas par défaut

    metafunc.parametrize("cas", selectionnes, scope="session")


# ─── option CLI optionnelle  ───────────────

def pytest_addoption(parser):
    parser.addoption(
        "--cas",
        action="store",
        default=None,
        help="Cas à exécuter : 'cas1' ou 'cas1,cas2'. Défaut = tous."
    )


# ─── fixtures dérivées de cas  ─────────────

@pytest.fixture(scope="session")
def fichier_cas(cas, config):
    return config["cas"][cas]["fichier"]


@pytest.fixture(scope="session")
def table_verification(cas, config):
    return config["cas"][cas]["table_verification"]


# ─── tag Allure dynamique sur chaque test  

@pytest.fixture(autouse=True)
def tag_cas(cas, config, request):
    nom     = config["cas"][cas]["nom"]
    fichier = config["cas"][cas]["fichier"].split("/")[-1]
    test    = request.node.name

    allure.dynamic.parent_suite("CB2C — PowerCARD Back-Office")
    allure.dynamic.suite(nom)
    allure.dynamic.sub_suite(f"{cas} — {fichier}")
    allure.dynamic.title(f"[{cas.upper()}] {test}")
    allure.dynamic.label("feature", "Validation CB2C")
    allure.dynamic.label("tag", cas)
    yield
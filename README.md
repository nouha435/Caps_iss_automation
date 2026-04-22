# Automatisation CB2C — PowerCARD

Framework de tests automatisés pour la validation du batch `cb2c_load_incoming.sh`.

---

## Structure du projet

```
.
├── config/
│   ├── config.yaml                  # Configuration principale
│   └── case_reason_mapping.yaml     # Mapping case_reason_code → libellé métier
│
├── utils/
│   ├── db_connector.py              # Connexion Oracle + execute_query()
│   ├── arn_calculator.py            # Calcul des ARNs depuis le fichier incoming
│   ├── clean_arn.py                 # Nettoyage Oracle par ARN
│   ├── clean_fichiers.py            # Nettoyage tables fichiers Oracle
│   └── clean_db.py                  # Façade re-exportant clean_par_arn et clean_tables_fichiers
│
├── tests/
│   └── test_tables/
│       ├── test_setup.py            # Etape 1 : fichier, clean, copie, batch
│       ├── test_trace.py            # Etape 2 : vérification trace
│       └── test_oracle.py           # Etape 3 : vérification Oracle
│
├── conftest.py                      # Fixtures pytest partagées
├── main.py                          # Orchestrateur principal
└── reports/
    └── allure/                      # Résultats bruts Allure (généré automatiquement)
```

---

## Cas disponibles

| Cas   | Nom                          | Table cible          | Stratégie clean |
|-------|------------------------------|----------------------|-----------------|
| cas1  | CB2C cas BON à PAYER         | TRANSACTION_HIST     | full            |
| cas2  | CB2C cas Représentation OK   | TRANSACTION_HIST_SUSP| tables_only     |

---

## Lancement

```bash
# Exécuter un cas
python3 main.py --cas cas1
python3 main.py --cas cas2

# Générer et ouvrir le rapport Allure
allure generate reports/allure -o reports/allure-html --clean
allure open reports/allure-html
```

---

## Détail des vérifications Oracle (`test_oracle.py`)

### Pour tous les cas — `test_arn_dans_table`

**Partie 1 — Présence ARN dans la table cible**
- `cas1` → vérifie dans `TRANSACTION_HIST`
- `cas2` → vérifie dans `TRANSACTION_HIST_SUSP`

**Partie 2 — Champs obligatoires sur les deux tables**

Les champs suivants sont vérifiés (non NULL) dans `TRANSACTION_HIST` **ET** `TRANSACTION_HIST_SUSP` :

| Champ                  | Description                        |
|------------------------|------------------------------------|
| `function_code`        | Code fonction de la transaction    |
| `reversal_flag`        | Indicateur d'annulation            |
| `transaction_code`     | Code type de transaction           |
| `microfilm_ref_number` | ARN — référence acquéreur          |

### Pour cas2 uniquement — `test_dossier_litige_representation`

| Etape | Table                  | Condition                          |
|-------|------------------------|------------------------------------|
| 1     | `case_table`           | `criteria_7 = '1'`                 |
| 2     | `EVENT_TABLE`          | `case_reason_code = '0004'`        |
| 3     | `LINK_EVENT_SCENARIO`  | via `event_reference`              |
| 4     | `case_reason`          | paramétrage `bank_code`            |
| 5     | `action_table`         | paramétrage actions `bank_code`    |

---

## Stratégies de nettoyage

| Stratégie     | clean par ARN | TRUNCATE tables buffer | DELETE pcrd_file_processing |
|---------------|:---:|:---:|:---:|
| `full`        | ✅  | ✅  | ✅  |
| `tables_only` | ❌  | ❌  | ✅  |
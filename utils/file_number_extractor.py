def extraire_file_number(fichier: str) -> str:
    """
    Lit la premiere ligne du fichier CB2C et retourne le FILE_NUMBER (6 chiffres)
    en extraction positionnelle col 22-27 inclus.
    Leve une ValueError si le header est absent ou mal forme.
    """
    with open(fichier, "r", encoding="utf-8", errors="replace") as f:
        premiere_ligne = f.readline().rstrip("\n")

    if not premiere_ligne:
        raise ValueError(f"Fichier vide ou illisible : {fichier}")

    if len(premiere_ligne) < 28:
        raise ValueError(
            f"Header trop court dans {fichier} "
            f"(longueur={len(premiere_ligne)}, minimum attendu=28) : "
            f"{premiere_ligne!r}"
        )

    file_number = premiere_ligne[22:28]

    if not file_number.isdigit():
        raise ValueError(
            f"FILE_NUMBER invalide extrait de {fichier} "
            f"col[22:28] = {file_number!r} (attendu : 6 chiffres numeriques)\n"
            f"Ligne header : {premiere_ligne!r}"
        )

    print(f"file_number extrait : {file_number}  (col 22-27 de la ligne header)")
    return file_number
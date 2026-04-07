def test_environnement_ok():
    """Vérifie que pytest fonctionne"""
    assert 1 + 1 == 2

def test_python_version():
    """Vérifie que Python 3 est utilisé"""
    import sys
    assert sys.version_info.major == 3
    print(f"\nVersion Python : {sys.version}")
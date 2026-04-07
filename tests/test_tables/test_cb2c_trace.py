import subprocess


def get_trace_content():
    result = subprocess.run(
        ["docker", "exec", "-u", "pwrcard", "CAPS-unix-3.5.4-RC2",
         "bash", "-c",
         "cat /pwrcard/home/usr/trace/cb2c_load_incoming.TRC000"],
        capture_output=True,
        text=True
    )
    return result.stdout


def extraire_compteurs(trace):
    processed = 0
    succeeded = 0
    rejected  = 0

    for ligne in trace.splitlines():
        if "Tx Processed" in ligne:
            processed = int(ligne.split(":")[-1].strip().split("]")[0].strip())
        elif "Tx Succeeded" in ligne:
            succeeded = int(ligne.split(":")[-1].strip().split("]")[0].strip())
        elif "Tx Rejected" in ligne:
            rejected  = int(ligne.split(":")[-1].strip().split("]")[0].strip())

    return processed, succeeded, rejected


def test_trace_batch_ok():
    trace = get_trace_content()
    assert trace, "fichier trace vide ou inaccessible"

    processed, succeeded, rejected = extraire_compteurs(trace)
    print(f"\nTx Processed : {processed}")
    print(f"Tx Succeeded : {succeeded}")
    print(f"Tx Rejected  : {rejected}")

    assert processed > 0,  "aucune transaction traitee"
    assert succeeded > 0,  "aucune transaction reussie"
    assert rejected  == 0, f"transactions rejetees : {rejected}"
    print("trace ok")
import subprocess
import os

def run_bdd_tests():
    print("Running BDD tests...")
    cwd = os.path.dirname(os.path.abspath(__file__))
    result = subprocess.run(["behave", "features"], cwd=cwd)
    if result.returncode != 0:
        raise Exception("BDD tests failed")

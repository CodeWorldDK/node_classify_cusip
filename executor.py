from services.catalog_service import get_dataset_coordinates
from services.persistor import persist_dataframe
from tests.snapshot_tests import run_snapshot_test
from tests.bdd.runner import run_bdd_tests

def execute_node(node_func, session, env="prod"):
    meta = node_func._node_meta
    inputs = {
        name: get_dataset_coordinates(name, env=env)
        for name in meta["inputs"]
    }

    params = meta["params"]
    result_df = node_func(session, inputs, params)

    output_table = get_dataset_coordinates(meta["output"], env=env)

    if env == "test":
        print("Running snapshot and BDD tests...")
        run_snapshot_test(node_func.__name__, session, inputs, params)
        run_bdd_tests()

    persist_dataframe(result_df, output_table)

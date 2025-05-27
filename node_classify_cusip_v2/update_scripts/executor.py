from snowflake.snowpark import Session
from tests.snapshot_tests import setup_snapshot_data, run_snapshot_test
from classify_cusip import classify_cusip_node
from utils.catalog_service import get_dataset_coordinates
from utils.persistor import persist_result

import os

def get_snowpark_session():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    return Session.builder.configs(connection_parameters).create()

def execute_node(node_name, env="prod", params=None):
    session = get_snowpark_session()
    print(f"Running node '{node_name}' in environment: {env}")
    if node_name == "classify_cusip":
        input_datasets = ["cusip_list", "product_category"]
        output_dataset = "classified_cusip"

        if env == "test":
            setup_snapshot_data(session, input_datasets, output_dataset)
            run_snapshot_test(
                node_name=node_name,
                session=session,
                inputs=input_datasets,
                params=params or {},
                test_schema="TEST"
            )
        else:
            # Use catalog service to resolve actual coordinates
            input_dfs = {}
            for name in input_datasets:
                dataset_path = get_dataset_coordinates(name, env=env)
                print(f"Resolved dataset '{name}' to '{dataset_path}'")
                input_dfs[name] = session.table(dataset_path)

            result_df = classify_cusip_node(session=session, **input_dfs, params=params or {})

            output_path = get_dataset_coordinates(output_dataset, env=env)
            print(f"Resolved output dataset to '{output_path}'")

            persist_result(result_df, output_path, session=session)
            print("Persisted results to output dataset")

if __name__ == "__main__":
    execute_node("classify_cusip", env="prod", params={"classification_ruleset": "R1"})

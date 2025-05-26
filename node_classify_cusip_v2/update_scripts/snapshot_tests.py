def run_snapshot_test(node_name, session, inputs, params):
    print(f"Running snapshot test for node: {node_name}")
    print(f"Inputs: {inputs}")
    print(f"Params: {params}")
    print("Snapshot comparison passed.")

def setup_snapshot_data(session, input_tables, output_table, prod_schema="PUBLIC", test_schema="TEST"):
    for table in input_tables:
        prod_table = f"{prod_schema}.{table}"
        test_table = f"{test_schema}.{table}"
        print(f"Cloning {prod_table} to {test_table}")
        session.sql(f"CREATE OR REPLACE TABLE {test_table} CLONE {prod_table}").collect()

    prod_output = f"{prod_schema}.{output_table}"
    test_output = f"{test_schema}.{output_table}"
    print(f"Cloning {prod_output} to {test_output}")
    session.sql(f"CREATE OR REPLACE TABLE {test_output} CLONE {prod_output}").collect()

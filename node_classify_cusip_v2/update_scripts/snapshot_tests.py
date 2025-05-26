def run_snapshot_test(node_name, session, inputs, params, test_schema="TEST"):
    print(f"Running snapshot test for node: {node_name}")

    # Build fully qualified table names for test input tables
    input_dfs = {}
    for table in inputs:
        test_table = f"{test_schema}.{table}"
        print(f"Reading test input table: {test_table}")
        input_dfs[table] = session.sql(f"SELECT * FROM {test_table}")

    # Dynamically import node module
    if node_name == "classify_cusip":
        from classify_cusip import classify_cusip_node
        result_df = classify_cusip_node(session=session, **input_dfs, params=params)
    else:
        raise Exception(f"Unknown node: {node_name}")

    # Read expected baseline output from snapshot
    output_table = f"{test_schema}.{node_name}_baseline"
    baseline_df = session.sql(f"SELECT * FROM {output_table}")

    # Dummy compare logic (should use DataFrame diff in real use)
    print("Comparing result with baseline...")
    result_rows = result_df.collect()
    baseline_rows = baseline_df.collect()

    if result_rows == baseline_rows:
        print("Snapshot test PASSED")
    else:
        print("Snapshot test FAILED")
        print("Result:")
        print(result_rows)
        print("Expected:")
        print(baseline_rows)

def setup_snapshot_data(session, input_tables, output_table, prod_schema="PUBLIC", test_schema="TEST"):
    for table in input_tables:
        prod_table = f"{prod_schema}.{table}"
        test_table = f"{test_schema}.{table}"
        print(f"Cloning {prod_table} to {test_table}")
        session.sql(f"CREATE OR REPLACE TABLE {test_table} CLONE {prod_table}").collect()

    prod_output = f"{prod_schema}.{output_table}"
    test_output = f"{test_schema}.{output_table}_baseline"
    print(f"Cloning {prod_output} to {test_output}")
    session.sql(f"CREATE OR REPLACE TABLE {test_output} CLONE {prod_output}").collect()

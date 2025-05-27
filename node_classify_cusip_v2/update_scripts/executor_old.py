from services.catalog_service import fetch_dataset_metadata
from services.persistor import persist_data
from tests.snapshot_tests import run_snapshot_test, setup_snapshot_data
from classify_cusip import classify_cusip_node

def execute_node(env="prod"):
    # Load metadata from decorator
    node_config = classify_cusip_node.__node_config__
    inputs = node_config["inputs"]
    output = node_config["output"]
    params = node_config.get("params", {})

    # Simulate session (Snowflake session object)
    session = DummySnowflakeSession()

    # Fetch dataset details from catalog
    catalog_inputs = fetch_dataset_metadata(inputs, env)
    catalog_output = fetch_dataset_metadata([output], env)[0]

    # Test environment setup
    if env == "test":
        setup_snapshot_data(session, inputs, output)
        run_snapshot_test("classify_cusip", session, inputs, params)

    # Run the transformation logic
    result = classify_cusip_node(session=session, **catalog_inputs, params=params)

    # Persist output
    persist_data(session, result, catalog_output)

# Dummy implementation for example purposes
class DummySnowflakeSession:
    def sql(self, query):
        print(f"[SQL]: {query}")
        return self

    def collect(self):
        print("[Collecting SQL Result]")
        return []

if __name__ == "__main__":
    execute_node(env="test")

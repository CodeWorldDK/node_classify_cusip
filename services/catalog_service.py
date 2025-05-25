DATASET_COORDINATES = {
    "cusip_list": "FINANCE_DB.PUBLIC.CUSIP_LIST",
    "product_category": "FINANCE_DB.PUBLIC.PRODUCT_CATEGORY",
    "cusip_classification": "FINANCE_DB.PUBLIC.CUSIP_CLASSIFICATION"
}

def get_dataset_coordinates(logical_name: str, env: str = "prod") -> str:
    return DATASET_COORDINATES.get(logical_name)

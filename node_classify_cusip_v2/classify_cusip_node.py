from snowflake.snowpark import Session, DataFrame
from decorators import node
from services.catalog_service import get_dataset_coordinates

@node(
    name="classify_cusip_node",
    inputs=["cusip_list", "product_category"],
    output="cusip_classification",
    params={"classification_ruleset": "R1"}
)
def classify_cusip(session: Session, inputs: dict, params: dict) -> DataFrame:
    cusip_table = get_dataset_coordinates("cusip_list")
    category_table = get_dataset_coordinates("product_category")

    cusips = session.table(cusip_table)
    categories = session.table(category_table)

    joined = cusips.join(categories, cusips["cusip"] == categories["cusip"])

    classified = joined.with_column(
        "classification",
        joined["product_category"].when("EQUITY", "EQ")
                                  .when("BOND", "FI")
                                  .otherwise("OTHER")
    ).select("cusip", "classification")

    return classified

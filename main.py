from snowflake.snowpark import Session
from classify_cusip_node import classify_cusip
from executor import execute_node

connection_params = {
    "account": "<your_account>",
    "user": "<your_user>",
    "password": "<your_password>",
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "FINANCE_DB",
    "schema": "PUBLIC"
}

session = Session.builder.configs(connection_params).create()
execute_node(classify_cusip, session, env="test")

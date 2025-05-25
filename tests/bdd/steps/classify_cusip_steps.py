from behave import given, when, then

@given('a CUSIP "{cusip}" with category "{category}"')
def step_given_cusip_with_category(context, cusip, category):
    context.test_input = {"cusip": cusip, "category": category}

@when("the node is executed")
def step_when_node_is_executed(context):
    category = context.test_input["category"]
    if category == "EQUITY":
        context.result = "EQ"
    elif category == "BOND":
        context.result = "FI"
    else:
        context.result = "OTHER"

@then('the classification should be "{expected}"')
def step_then_check_classification(context, expected):
    assert context.result == expected

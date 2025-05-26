Feature: Classify CUSIP based on product category

  Scenario: Classify EQUITY cusips
    Given a CUSIP "12345678" with category "EQUITY"
    When the node is executed
    Then the classification should be "EQ"

  Scenario: Classify BOND cusips
    Given a CUSIP "87654321" with category "BOND"
    When the node is executed
    Then the classification should be "FI"

  Scenario: Classify OTHER category
    Given a CUSIP "55555555" with category "DERIVATIVE"
    When the node is executed
    Then the classification should be "OTHER"

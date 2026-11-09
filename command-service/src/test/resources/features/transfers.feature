Feature: Transfers between accounts
  As a bank customer
  I want to move money between accounts
  So that payments are applied exactly once and never overdraw an account

  Background:
    Given "Alice" has a "USD" account with a balance of 100.00
    And "Bob" has a "USD" account with a balance of 20.00

  Scenario: A transfer within the available balance succeeds
    When "Alice" transfers 30.00 to "Bob"
    Then the transfer is accepted
    And "Alice" has a balance of 70.00
    And "Bob" has a balance of 50.00

  Scenario: A transfer larger than the balance is rejected
    When "Alice" transfers 150.00 to "Bob"
    Then the transfer is rejected with "INSUFFICIENT_FUNDS"
    And "Alice" has a balance of 100.00
    And "Bob" has a balance of 20.00

  Scenario: Transferring the exact balance empties the account
    When "Alice" transfers 100.00 to "Bob"
    Then the transfer is accepted
    And "Alice" has a balance of 0.00

  Scenario: A transfer to an account in another currency is rejected
    Given "Chloe" has a "EUR" account with a balance of 10.00
    When "Alice" transfers 5.00 to "Chloe"
    Then the transfer is rejected with "CURRENCY_MISMATCH"

  Scenario: A client retry with the same idempotency key is applied once
    When "Alice" transfers 40.00 to "Bob" with idempotency key "pay-123"
    And "Alice" transfers 40.00 to "Bob" with idempotency key "pay-123"
    Then both requests return the same transfer id
    And "Alice" has a balance of 60.00
    And "Bob" has a balance of 60.00

  Scenario Outline: Invalid amounts are rejected
    When "Alice" transfers <amount> to "Bob"
    Then the transfer is rejected with "<code>"

    Examples:
      | amount     | code           |
      | 0.00       | INVALID_AMOUNT |
      | -10.00     | INVALID_AMOUNT |
      | 1.005      | INVALID_AMOUNT |
      | 1000000.01 | LIMIT_EXCEEDED |

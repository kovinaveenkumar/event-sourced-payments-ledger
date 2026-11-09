Feature: Balances are derived from the event history
  The ledger never stores a balance on the write side. It stores events,
  and the balance is whatever replaying those events produces.

  Scenario: Replaying the history reproduces the balance
    Given "Dana" has a "USD" account with a balance of 50.00
    When "Dana" deposits 25.00
    And "Dana" withdraws 10.00
    Then the history of "Dana" contains these events in order:
      | AccountOpened  |
      | FundsDeposited |
      | FundsDeposited |
      | FundsWithdrawn |
    And "Dana" has a balance of 65.00

  Scenario: A rejected command leaves no trace in the history
    Given "Eve" has a "USD" account with a balance of 5.00
    When "Eve" withdraws 6.00
    Then the command is rejected with "INSUFFICIENT_FUNDS"
    And the history of "Eve" contains these events in order:
      | AccountOpened  |
      | FundsDeposited |

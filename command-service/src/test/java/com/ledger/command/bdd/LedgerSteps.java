package com.ledger.command.bdd;

import com.ledger.command.application.LedgerCommandService;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.DomainException;
import com.ledger.command.domain.Money;
import com.ledger.command.store.InMemoryEventStore;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cucumber steps. They call the real service with the in-memory store, so the scenarios
 * test the same rules the app uses, without needing a database.
 */
public class LedgerSteps {

    private LedgerCommandService service;
    private final Map<String, UUID> accounts = new HashMap<>();
    private final List<UUID> results = new ArrayList<>();
    private DomainException lastError;

    @Before
    public void setUp() {
        service = new LedgerCommandService(new InMemoryEventStore(), Clock.systemUTC());
        accounts.clear();
        results.clear();
        lastError = null;
    }

    @Given("{string} has a {string} account with a balance of {bigdecimal}")
    public void hasAccount(String owner, String currency, BigDecimal balance) {
        accounts.put(owner, service.openAccount(owner, currency, balance, null));
    }

    @When("{string} transfers {bigdecimal} to {string}")
    public void transfers(String from, BigDecimal amount, String to) {
        run(() -> service.transfer(accounts.get(from), accounts.get(to), amount, null));
    }

    @When("{string} transfers {bigdecimal} to {string} with idempotency key {string}")
    public void transfersWithKey(String from, BigDecimal amount, String to, String key) {
        run(() -> service.transfer(accounts.get(from), accounts.get(to), amount, key));
    }

    @When("{string} deposits {bigdecimal}")
    public void deposits(String owner, BigDecimal amount) {
        run(() -> service.deposit(accounts.get(owner), amount, "bdd", null));
    }

    @When("{string} withdraws {bigdecimal}")
    public void withdraws(String owner, BigDecimal amount) {
        run(() -> service.withdraw(accounts.get(owner), amount, "bdd", null));
    }

    @Then("the transfer is accepted")
    public void accepted() {
        assertThat(lastError).as("unexpected rejection").isNull();
        assertThat(results).isNotEmpty();
    }

    @Then("the transfer is rejected with {string}")
    @Then("the command is rejected with {string}")
    public void rejected(String code) {
        assertThat(lastError).as("expected a rejection").isNotNull();
        assertThat(lastError.code().name()).isEqualTo(code);
    }

    @Then("both requests return the same transfer id")
    public void sameTransferId() {
        assertThat(results).hasSize(2);
        assertThat(results.get(1)).isEqualTo(results.get(0));
    }

    @Then("{string} has a balance of {bigdecimal}")
    public void hasBalance(String owner, BigDecimal expected) {
        long cents = service.currentState(accounts.get(owner)).balanceCents();
        assertThat(Money.toDecimal(cents)).isEqualByComparingTo(expected);
    }

    @Then("the history of {string} contains these events in order:")
    public void historyContains(String owner, List<String> types) {
        assertThat(service.history(accounts.get(owner))).extracting(AccountEvent::type).containsExactlyElementsOf(types);
    }

    private void run(java.util.function.Supplier<UUID> command) {
        try {
            results.add(command.get());
        } catch (DomainException e) {
            lastError = e;
        }
    }
}

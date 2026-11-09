package com.ledger.command.domain;

import com.ledger.command.domain.AccountEvent.AccountOpened;
import com.ledger.command.domain.AccountEvent.FundsDeposited;
import com.ledger.command.domain.AccountEvent.TransferSent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AccountTest {

    private final Clock clock = Clock.fixed(Instant.parse("2026-01-15T10:00:00Z"), ZoneOffset.UTC);

    private Account openWith(String amount) {
        UUID id = UUID.randomUUID();
        AccountOpened opened = Account.open(id, "Alice", "USD", clock);
        Account account = Account.empty(id).apply(opened);
        if (amount != null) {
            account = account.apply(account.deposit(usd(amount), "seed", clock));
        }
        return account;
    }

    private static Money usd(String amount) {
        return Money.of(new BigDecimal(amount), "USD");
    }

    @Test
    void replayingHistoryRebuildsBalanceAndVersion() {
        UUID id = UUID.randomUUID();
        Instant t = clock.instant();
        List<AccountEvent> history = List.of(
                new AccountOpened(UUID.randomUUID(), id, 1, t, "Alice", "USD"),
                new FundsDeposited(UUID.randomUUID(), id, 2, t, 10_000, "pay"),
                new AccountEvent.FundsWithdrawn(UUID.randomUUID(), id, 3, t, 2_550, "atm"),
                new AccountEvent.TransferReceived(UUID.randomUUID(), id, 4, t, UUID.randomUUID(), UUID.randomUUID(), 50));

        Account account = Account.replay(id, history);

        assertThat(account.balanceCents()).isEqualTo(7_500);
        assertThat(account.version()).isEqualTo(4);
        assertThat(account.owner()).isEqualTo("Alice");
    }

    @Test
    void replayRejectsGapsInTheStream() {
        UUID id = UUID.randomUUID();
        List<AccountEvent> history = List.of(
                new AccountOpened(UUID.randomUUID(), id, 1, clock.instant(), "Alice", "USD"),
                new FundsDeposited(UUID.randomUUID(), id, 3, clock.instant(), 100, "skipped v2"));

        assertThatThrownBy(() -> Account.replay(id, history)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void depositProducesNextVersionWithoutMutatingState() {
        Account account = openWith("100.00");

        FundsDeposited event = account.deposit(usd("25.50"), "salary", clock);

        assertThat(event.version()).isEqualTo(account.version() + 1);
        assertThat(event.amountCents()).isEqualTo(2_550);
        assertThat(account.balanceCents()).isEqualTo(10_000);
    }

    @Test
    void withdrawalBeyondBalanceIsRejected() {
        Account account = openWith("100.00");

        assertThatThrownBy(() -> account.withdraw(usd("100.01"), "atm", clock))
                .isInstanceOf(DomainException.class)
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.INSUFFICIENT_FUNDS);
    }

    @Test
    void withdrawingTheExactBalanceIsAllowed() {
        Account account = openWith("100.00");
        Account after = account.apply(account.withdraw(usd("100.00"), "atm", clock));
        assertThat(after.balanceCents()).isZero();
    }

    @Test
    void transferCreatesMatchingDebitAndCredit() {
        Account from = openWith("200.00");
        Account to = openWith(null);
        UUID transferId = UUID.randomUUID();

        TransferSent sent = from.sendTransfer(transferId, to, usd("75.00"), clock);
        AccountEvent.TransferReceived received = to.receiveTransfer(sent, clock);

        assertThat(sent.counterpartyAccountId()).isEqualTo(to.id());
        assertThat(received.counterpartyAccountId()).isEqualTo(from.id());
        assertThat(received.transferId()).isEqualTo(transferId);
        assertThat(from.apply(sent).balanceCents() + to.apply(received).balanceCents()).isEqualTo(20_000);
    }

    @Test
    void transferBetweenCurrenciesIsRejected() {
        Account usd = openWith("50.00");
        UUID eurId = UUID.randomUUID();
        Account eur = Account.empty(eurId).apply(Account.open(eurId, "Bob", "EUR", clock));

        assertThatThrownBy(() -> usd.sendTransfer(UUID.randomUUID(), eur, usd("10.00"), clock))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.CURRENCY_MISMATCH);
    }

    @Test
    void amountsAboveTheSingleTransactionLimitAreRejected() {
        Account account = openWith(null);
        assertThatThrownBy(() -> account.deposit(usd("1000000.01"), "big", clock))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.LIMIT_EXCEEDED);
    }

    @Test
    void cannotOperateOnAnAccountThatWasNeverOpened() {
        Account ghost = Account.empty(UUID.randomUUID());
        assertThatThrownBy(() -> ghost.deposit(usd("1.00"), "x", clock))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.ACCOUNT_NOT_FOUND);
    }

    @Test
    void openingRequiresOwnerAndValidCurrency() {
        assertThatThrownBy(() -> Account.open(UUID.randomUUID(), " ", "USD", clock))
                .isInstanceOf(DomainException.class);
        assertThatThrownBy(() -> Account.open(UUID.randomUUID(), "Alice", "XYZ", clock))
                .isInstanceOf(DomainException.class);
    }
}

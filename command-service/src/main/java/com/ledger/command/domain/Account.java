package com.ledger.command.domain;

import com.ledger.command.domain.AccountEvent.AccountOpened;
import com.ledger.command.domain.AccountEvent.FundsDeposited;
import com.ledger.command.domain.AccountEvent.FundsWithdrawn;
import com.ledger.command.domain.AccountEvent.TransferReceived;
import com.ledger.command.domain.AccountEvent.TransferSent;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * An account. It never changes in place: I build it by replaying its past events, and
 * methods like deposit() check the rules and return a new event. Saving the event is
 * done by the caller.
 */
public final class Account {

    /** Biggest single amount allowed: 1,000,000.00. */
    public static final long MAX_SINGLE_AMOUNT_CENTS = 100_000_000L;

    private final UUID id;
    private final String owner;
    private final String currency;
    private final long balanceCents;
    private final long version;

    private Account(UUID id, String owner, String currency, long balanceCents, long version) {
        this.id = id;
        this.owner = owner;
        this.currency = currency;
        this.balanceCents = balanceCents;
        this.version = version;
    }

    public static Account empty(UUID id) {
        return new Account(id, null, null, 0L, 0L);
    }

    /** Builds the current state by applying each past event in order. */
    public static Account replay(UUID id, List<AccountEvent> history) {
        Account state = empty(id);
        for (AccountEvent event : history) {
            state = state.apply(event);
        }
        return state;
    }

    public Account apply(AccountEvent event) {
        if (event.version() != version + 1) {
            throw new IllegalStateException("Out-of-order event " + event.version() + " for account " + id
                    + " at version " + version);
        }
        if (event instanceof AccountOpened e) {
            return new Account(id, e.owner(), e.currency(), 0L, e.version());
        } else if (event instanceof FundsDeposited e) {
            return withBalance(balanceCents + e.amountCents(), e.version());
        } else if (event instanceof FundsWithdrawn e) {
            return withBalance(balanceCents - e.amountCents(), e.version());
        } else if (event instanceof TransferSent e) {
            return withBalance(balanceCents - e.amountCents(), e.version());
        } else if (event instanceof TransferReceived e) {
            return withBalance(balanceCents + e.amountCents(), e.version());
        }
        throw new IllegalArgumentException("Unknown event " + event);
    }

    // commands: check the rules and return the new event

    public static AccountOpened open(UUID accountId, String owner, String currency, Clock clock) {
        if (owner == null || owner.isBlank()) {
            throw new DomainException(ErrorCode.INVALID_REQUEST, "Owner is required");
        }
        Money.validateCurrency(currency);
        return new AccountOpened(UUID.randomUUID(), accountId, 1L, Instant.now(clock), owner.trim(), currency);
    }

    public FundsDeposited deposit(Money amount, String reference, Clock clock) {
        requireOpen();
        requireCurrency(amount);
        requireWithinLimit(amount);
        return new FundsDeposited(UUID.randomUUID(), id, version + 1, Instant.now(clock), amount.cents(), reference);
    }

    public FundsWithdrawn withdraw(Money amount, String reference, Clock clock) {
        requireOpen();
        requireCurrency(amount);
        requireWithinLimit(amount);
        requireFunds(amount);
        return new FundsWithdrawn(UUID.randomUUID(), id, version + 1, Instant.now(clock), amount.cents(), reference);
    }

    public TransferSent sendTransfer(UUID transferId, Account target, Money amount, Clock clock) {
        requireOpen();
        target.requireOpen();
        if (id.equals(target.id)) {
            throw new DomainException(ErrorCode.SAME_ACCOUNT_TRANSFER, "Cannot transfer to the same account");
        }
        requireCurrency(amount);
        if (!currency.equals(target.currency)) {
            throw new DomainException(ErrorCode.CURRENCY_MISMATCH,
                    "Source account is " + currency + " but target account is " + target.currency);
        }
        requireWithinLimit(amount);
        requireFunds(amount);
        return new TransferSent(UUID.randomUUID(), id, version + 1, Instant.now(clock), transferId, target.id,
                amount.cents());
    }

    public TransferReceived receiveTransfer(TransferSent sent, Clock clock) {
        requireOpen();
        return new TransferReceived(UUID.randomUUID(), id, version + 1, Instant.now(clock), sent.transferId(),
                sent.accountId(), sent.amountCents());
    }

    // rule checks

    private void requireOpen() {
        if (!exists()) {
            throw new DomainException(ErrorCode.ACCOUNT_NOT_FOUND, "Account " + id + " does not exist");
        }
    }

    private void requireCurrency(Money amount) {
        if (!currency.equals(amount.currency())) {
            throw new DomainException(ErrorCode.CURRENCY_MISMATCH,
                    "Account " + id + " holds " + currency + ", not " + amount.currency());
        }
    }

    private void requireWithinLimit(Money amount) {
        if (amount.cents() > MAX_SINGLE_AMOUNT_CENTS) {
            throw new DomainException(ErrorCode.LIMIT_EXCEEDED, "Amount exceeds the single-transaction limit of "
                    + Money.toDecimal(MAX_SINGLE_AMOUNT_CENTS));
        }
    }

    private void requireFunds(Money amount) {
        if (balanceCents < amount.cents()) {
            throw new DomainException(ErrorCode.INSUFFICIENT_FUNDS, "Balance " + Money.toDecimal(balanceCents)
                    + " is less than " + Money.toDecimal(amount.cents()));
        }
    }

    private Account withBalance(long newBalance, long newVersion) {
        return new Account(id, owner, currency, newBalance, newVersion);
    }

    public boolean exists() {
        return version > 0;
    }

    public UUID id() {
        return id;
    }

    public String owner() {
        return owner;
    }

    public String currency() {
        return currency;
    }

    public long balanceCents() {
        return balanceCents;
    }

    public long version() {
        return version;
    }
}

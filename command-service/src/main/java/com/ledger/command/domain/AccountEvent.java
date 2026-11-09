package com.ledger.command.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Every change to an account is saved as one of these events. The events are the real
 * record, and balances are worked out from them.
 *
 * I made the interface sealed so the compiler knows every event type.
 */
public sealed interface AccountEvent {

    UUID eventId();

    UUID accountId();

    /** 1 for the first event of an account, 2 for the next, and so on. */
    long version();

    Instant occurredAt();

    default String type() {
        return getClass().getSimpleName();
    }

    record AccountOpened(UUID eventId, UUID accountId, long version, Instant occurredAt,
                         String owner, String currency) implements AccountEvent {
    }

    record FundsDeposited(UUID eventId, UUID accountId, long version, Instant occurredAt,
                          long amountCents, String reference) implements AccountEvent {
    }

    record FundsWithdrawn(UUID eventId, UUID accountId, long version, Instant occurredAt,
                          long amountCents, String reference) implements AccountEvent {
    }

    record TransferSent(UUID eventId, UUID accountId, long version, Instant occurredAt,
                        UUID transferId, UUID counterpartyAccountId, long amountCents) implements AccountEvent {
    }

    record TransferReceived(UUID eventId, UUID accountId, long version, Instant occurredAt,
                            UUID transferId, UUID counterpartyAccountId, long amountCents) implements AccountEvent {
    }
}

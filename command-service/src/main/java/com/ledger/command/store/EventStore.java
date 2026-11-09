package com.ledger.command.store;

import com.ledger.command.domain.AccountEvent;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Where events are saved. Events are only ever added, never changed. Every version
 * of this store has to:
 * - refuse a version that already exists for an account (ConcurrencyConflictException),
 * - save all events from one append() call together, or none of them,
 * - refuse an Idempotency-Key that was already used (DuplicateCommandException).
 */
public interface EventStore {

    List<AccountEvent> load(UUID accountId);

    void append(List<AccountEvent> events, IdempotencyRecord idempotency);

    Optional<IdempotencyRecord> findIdempotency(String key);

    /** Events not sent to Kafka yet, oldest first. */
    List<StoredEvent> fetchUnpublished(int limit);

    void markPublished(List<Long> sequences);

    record StoredEvent(long sequence, AccountEvent event) {
    }

    /**
     * Remembers the result of a request sent with an Idempotency-Key, so a retry gets the
     * same answer and money doesn't move twice.
     *
     * @param fingerprint hash of the request, so I can tell if the same key is sent with a different request
     */
    record IdempotencyRecord(String key, String fingerprint, UUID resultId) {
    }
}

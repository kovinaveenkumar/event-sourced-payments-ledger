package com.ledger.command.store;

import com.ledger.command.domain.AccountEvent;
import com.ledger.command.messaging.EventCodec;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Postgres event store. The UNIQUE (account_id, version) rule stops two requests from
 * both saving the same version of an account. The events table is also my outbox:
 * published_at is empty until the event has been sent to Kafka.
 */
public class JdbcEventStore implements EventStore {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final EventCodec codec;

    public JdbcEventStore(JdbcTemplate jdbc, TransactionTemplate tx, EventCodec codec) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.codec = codec;
    }

    @Override
    public List<AccountEvent> load(UUID accountId) {
        return jdbc.query("SELECT payload FROM events WHERE account_id = ? ORDER BY version",
                (rs, i) -> codec.decode(rs.getString("payload")), accountId);
    }

    /**
     * I insert events sorted by account id. Before this, a transfer A to B and a transfer
     * B to A at the same time could each wait for the other forever (a deadlock). I found
     * this with the load test. If a lock error still happens, it is retried.
     */
    @Override
    public void append(List<AccountEvent> events, IdempotencyRecord record) {
        List<AccountEvent> ordered = events.stream()
                .sorted(LOCK_ORDER)
                .toList();
        try {
            insertAll(ordered, record);
        } catch (PessimisticLockingFailureException e) {
            throw new ConcurrencyConflictException("Lock conflict while appending: " + e.getMostSpecificCause().getMessage());
        }
    }

    private static final Comparator<AccountEvent> LOCK_ORDER =
            Comparator.comparing(AccountEvent::accountId).thenComparingLong(AccountEvent::version);

    private void insertAll(List<AccountEvent> events, IdempotencyRecord record) {
        tx.executeWithoutResult(status -> {
            if (record != null) {
                int inserted = jdbc.update("""
                        INSERT INTO idempotency_keys (idempotency_key, fingerprint, result_id)
                        VALUES (?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING
                        """, record.key(), record.fingerprint(), record.resultId());
                if (inserted == 0) {
                    throw new DuplicateCommandException(record.key());
                }
            }
            for (AccountEvent e : events) {
                try {
                    jdbc.update("""
                            INSERT INTO events (event_id, account_id, version, event_type, payload, occurred_at)
                            VALUES (?, ?, ?, ?, ?::jsonb, ?)
                            """, e.eventId(), e.accountId(), e.version(), e.type(), codec.encode(e),
                            Timestamp.from(e.occurredAt()));
                } catch (DuplicateKeyException ex) {
                    throw new ConcurrencyConflictException("Version " + e.version() + " of account "
                            + e.accountId() + " was already written by another command");
                }
            }
        });
    }

    @Override
    public Optional<IdempotencyRecord> findIdempotency(String key) {
        return jdbc.query("SELECT idempotency_key, fingerprint, result_id FROM idempotency_keys WHERE idempotency_key = ?",
                (rs, i) -> new IdempotencyRecord(rs.getString(1), rs.getString(2), rs.getObject(3, UUID.class)), key)
                .stream().findFirst();
    }

    @Override
    public List<StoredEvent> fetchUnpublished(int limit) {
        return jdbc.query("""
                        SELECT global_seq, payload FROM events
                        WHERE published_at IS NULL ORDER BY global_seq LIMIT ?
                        """,
                (rs, i) -> new StoredEvent(rs.getLong("global_seq"), codec.decode(rs.getString("payload"))), limit);
    }

    @Override
    public void markPublished(List<Long> sequences) {
        jdbc.batchUpdate("UPDATE events SET published_at = now() WHERE global_seq = ?",
                sequences.stream().map(s -> new Object[]{s}).toList());
    }
}

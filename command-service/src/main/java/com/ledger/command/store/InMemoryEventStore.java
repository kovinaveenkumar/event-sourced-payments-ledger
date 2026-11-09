package com.ledger.command.store;

import com.ledger.command.domain.AccountEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * In-memory version of the event store with the same rules as the Postgres one.
 * I use it in unit tests and Cucumber tests so they run fast without a database.
 */
public class InMemoryEventStore implements EventStore {

    private final List<AccountEvent> log = new ArrayList<>();
    private final Map<UUID, List<AccountEvent>> streams = new HashMap<>();
    private final Map<String, IdempotencyRecord> idempotency = new HashMap<>();
    private final Set<Long> published = new HashSet<>();

    @Override
    public synchronized List<AccountEvent> load(UUID accountId) {
        return List.copyOf(streams.getOrDefault(accountId, List.of()));
    }

    @Override
    public synchronized void append(List<AccountEvent> events, IdempotencyRecord record) {
        if (record != null && idempotency.containsKey(record.key())) {
            throw new DuplicateCommandException(record.key());
        }
        // Check everything first, then write, so it's all or nothing.
        Map<UUID, Long> expected = new HashMap<>();
        for (AccountEvent e : events) {
            long current = expected.computeIfAbsent(e.accountId(),
                    id -> (long) streams.getOrDefault(id, List.of()).size());
            if (e.version() != current + 1) {
                throw new ConcurrencyConflictException("Account " + e.accountId() + " is at version " + current
                        + ", cannot append version " + e.version());
            }
            expected.put(e.accountId(), e.version());
        }
        for (AccountEvent e : events) {
            streams.computeIfAbsent(e.accountId(), id -> new ArrayList<>()).add(e);
            log.add(e);
        }
        if (record != null) {
            idempotency.put(record.key(), record);
        }
    }

    @Override
    public synchronized Optional<IdempotencyRecord> findIdempotency(String key) {
        return Optional.ofNullable(idempotency.get(key));
    }

    @Override
    public synchronized List<StoredEvent> fetchUnpublished(int limit) {
        List<StoredEvent> result = new ArrayList<>();
        for (int i = 0; i < log.size() && result.size() < limit; i++) {
            long seq = i + 1L;
            if (!published.contains(seq)) {
                result.add(new StoredEvent(seq, log.get(i)));
            }
        }
        return result;
    }

    @Override
    public synchronized void markPublished(List<Long> sequences) {
        published.addAll(sequences);
    }
}

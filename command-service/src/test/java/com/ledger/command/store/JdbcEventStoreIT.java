package com.ledger.command.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.command.application.LedgerCommandService;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.AccountEvent.AccountOpened;
import com.ledger.command.domain.AccountEvent.FundsDeposited;
import com.ledger.command.domain.DomainException;
import com.ledger.command.messaging.EventCodec;
import com.ledger.command.store.EventStore.IdempotencyRecord;
import com.ledger.command.store.EventStore.StoredEvent;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs the real Postgres event store against Postgres in Docker. The unit tests use the
 * in-memory store, so these tests make sure the SQL, the unique constraints and the
 * transactions really behave the way the in-memory version promises.
 */
@Testcontainers
class JdbcEventStoreIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static HikariDataSource dataSource;
    private static JdbcTemplate jdbc;
    private static JdbcEventStore store;

    @BeforeAll
    static void setUp() {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(POSTGRES.getJdbcUrl());
        dataSource.setUsername(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        dataSource.setMaximumPoolSize(20);
        Flyway.configure().dataSource(dataSource).load().migrate();

        jdbc = new JdbcTemplate(dataSource);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        store = new JdbcEventStore(jdbc, tx, new EventCodec(new ObjectMapper()));
    }

    @AfterAll
    static void tearDown() {
        dataSource.close();
    }

    @BeforeEach
    void cleanTables() {
        jdbc.execute("TRUNCATE events, idempotency_keys RESTART IDENTITY");
    }

    private static AccountOpened opened(UUID id) {
        return new AccountOpened(UUID.randomUUID(), id, 1, Instant.now(), "Alice", "USD");
    }

    private static FundsDeposited deposit(UUID id, long version, long cents) {
        return new FundsDeposited(UUID.randomUUID(), id, version, Instant.now(), cents, "test");
    }

    @Test
    void savesAndLoadsEventsInVersionOrder() {
        UUID id = UUID.randomUUID();
        store.append(List.of(opened(id), deposit(id, 2, 500)), null);
        store.append(List.of(deposit(id, 3, 250)), null);

        List<AccountEvent> loaded = store.load(id);

        assertThat(loaded).extracting(AccountEvent::version).containsExactly(1L, 2L, 3L);
        assertThat(((FundsDeposited) loaded.get(2)).amountCents()).isEqualTo(250);
    }

    @Test
    void sameVersionTwiceIsAConflict() {
        UUID id = UUID.randomUUID();
        store.append(List.of(opened(id)), null);
        store.append(List.of(deposit(id, 2, 100)), null);

        assertThatThrownBy(() -> store.append(List.of(deposit(id, 2, 999)), null))
                .isInstanceOf(ConcurrencyConflictException.class);
        assertThat(store.load(id)).hasSize(2);
    }

    @Test
    void aFailedBatchSavesNothing() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        store.append(List.of(opened(a), opened(b), deposit(b, 2, 100)), null);

        // a's event is fine, but b already has version 2, so the whole batch must roll back
        assertThatThrownBy(() -> store.append(List.of(deposit(a, 2, 50), deposit(b, 2, 50)), null))
                .isInstanceOf(ConcurrencyConflictException.class);
        assertThat(store.load(a)).hasSize(1);
    }

    @Test
    void reusedIdempotencyKeyIsRejectedAndSavesNothing() {
        UUID id = UUID.randomUUID();
        store.append(List.of(opened(id)), new IdempotencyRecord("key-1", "abc", id));

        assertThatThrownBy(() -> store.append(List.of(deposit(id, 2, 100)), new IdempotencyRecord("key-1", "abc", id)))
                .isInstanceOf(DuplicateCommandException.class);
        assertThat(store.load(id)).hasSize(1);
        assertThat(store.findIdempotency("key-1")).get().extracting(IdempotencyRecord::resultId).isEqualTo(id);
    }

    @Test
    void outboxReturnsUnsentEventsOldestFirstUntilMarked() {
        UUID id = UUID.randomUUID();
        store.append(List.of(opened(id), deposit(id, 2, 100), deposit(id, 3, 200)), null);

        List<StoredEvent> pending = store.fetchUnpublished(10);
        assertThat(pending).extracting(s -> s.event().version()).containsExactly(1L, 2L, 3L);

        store.markPublished(List.of(pending.get(0).sequence(), pending.get(1).sequence()));
        assertThat(store.fetchUnpublished(10)).extracting(s -> s.event().version()).containsExactly(3L);
    }

    /**
     * The bug I found with the load test: transfers A to B and B to A at the same time used
     * to deadlock in Postgres. This fires 200 of them both ways at once and expects no errors
     * other than normal business rejections, and the total money to stay the same.
     */
    @Test
    void opposingTransfersAtTheSameTimeDoNotDeadlock() throws Exception {
        LedgerCommandService service = new LedgerCommandService(store, Clock.systemUTC());
        UUID alice = service.openAccount("Alice", "USD", new BigDecimal("5000.00"), null);
        UUID bob = service.openAccount("Bob", "USD", new BigDecimal("5000.00"), null);

        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<?>> results = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            boolean forward = i % 2 == 0;
            results.add(pool.submit(() -> {
                try {
                    service.transfer(forward ? alice : bob, forward ? bob : alice, new BigDecimal("7.25"), null);
                } catch (DomainException e) {
                    // insufficient funds or a conflict after all retries are allowed answers
                }
            }));
        }
        for (Future<?> f : results) {
            f.get(60, TimeUnit.SECONDS); // any deadlock or SQL error would surface here
        }
        pool.shutdown();

        long total = service.currentState(alice).balanceCents() + service.currentState(bob).balanceCents();
        assertThat(total).isEqualTo(1_000_000L);
    }

    @Test
    void concurrentWithdrawalsNeverOverdraw() throws Exception {
        LedgerCommandService service = new LedgerCommandService(store, Clock.systemUTC());
        UUID alice = service.openAccount("Alice", "USD", new BigDecimal("100.00"), null);

        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            results.add(pool.submit(() -> {
                try {
                    service.withdraw(alice, new BigDecimal("10.00"), "race", null);
                    return true;
                } catch (DomainException e) {
                    return false;
                }
            }));
        }
        long succeeded = 0;
        for (Future<Boolean> f : results) {
            if (f.get(60, TimeUnit.SECONDS)) {
                succeeded++;
            }
        }
        pool.shutdown();

        long balance = service.currentState(alice).balanceCents();
        assertThat(balance).isGreaterThanOrEqualTo(0);
        assertThat(balance).isEqualTo(10_000 - succeeded * 1_000);
    }
}

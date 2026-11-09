package com.ledger.command.application;

import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.DomainException;
import com.ledger.command.domain.ErrorCode;
import com.ledger.command.store.ConcurrencyConflictException;
import com.ledger.command.store.EventStore;
import com.ledger.command.store.InMemoryEventStore;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LedgerCommandServiceTest {

    private final InMemoryEventStore store = new InMemoryEventStore();
    private final LedgerCommandService service = new LedgerCommandService(store, Clock.systemUTC());

    private UUID open(String owner, String amount) {
        return service.openAccount(owner, "USD", amount == null ? null : new BigDecimal(amount), null);
    }

    private long balance(UUID id) {
        return service.currentState(id).balanceCents();
    }

    @Test
    void openAccountWithInitialDepositWritesTwoEvents() {
        UUID id = open("Alice", "100.00");

        List<AccountEvent> history = service.history(id);
        assertThat(history).extracting(AccountEvent::type).containsExactly("AccountOpened", "FundsDeposited");
        assertThat(balance(id)).isEqualTo(10_000);
    }

    @Test
    void transferMovesMoneyAndConservesTheTotal() {
        UUID alice = open("Alice", "100.00");
        UUID bob = open("Bob", "20.00");

        service.transfer(alice, bob, new BigDecimal("30.00"), null);

        assertThat(balance(alice)).isEqualTo(7_000);
        assertThat(balance(bob)).isEqualTo(5_000);
        assertThat(balance(alice) + balance(bob)).isEqualTo(12_000);
    }

    @Test
    void rejectedTransferWritesNothing() {
        UUID alice = open("Alice", "100.00");
        UUID bob = open("Bob", null);
        int eventsBefore = store.fetchUnpublished(1_000).size();

        assertThatThrownBy(() -> service.transfer(alice, bob, new BigDecimal("150.00"), null))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.INSUFFICIENT_FUNDS);

        assertThat(store.fetchUnpublished(1_000)).hasSize(eventsBefore);
    }

    @Test
    void transferToUnknownAccountIsNotFound() {
        UUID alice = open("Alice", "100.00");
        assertThatThrownBy(() -> service.transfer(alice, UUID.randomUUID(), BigDecimal.ONE, null))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.ACCOUNT_NOT_FOUND);
    }

    @Test
    void retryingWithSameIdempotencyKeyDoesNotMoveMoneyTwice() {
        UUID alice = open("Alice", "100.00");
        UUID bob = open("Bob", null);

        UUID first = service.transfer(alice, bob, new BigDecimal("40.00"), "key-1");
        UUID retry = service.transfer(alice, bob, new BigDecimal("40.00"), "key-1");

        assertThat(retry).isEqualTo(first);
        assertThat(balance(alice)).isEqualTo(6_000);
        assertThat(balance(bob)).isEqualTo(4_000);
    }

    @Test
    void reusingAnIdempotencyKeyForADifferentRequestIsRejected() {
        UUID alice = open("Alice", "100.00");
        UUID bob = open("Bob", null);
        service.transfer(alice, bob, new BigDecimal("40.00"), "key-2");

        assertThatThrownBy(() -> service.transfer(alice, bob, new BigDecimal("41.00"), "key-2"))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.IDEMPOTENCY_KEY_REUSED);
    }

    @Test
    void retriesOnConcurrencyConflictThenSucceeds() {
        AtomicInteger failuresLeft = new AtomicInteger(2);
        EventStore flaky = new DelegatingStore(store) {
            @Override
            public void append(List<AccountEvent> events, IdempotencyRecord idempotency) {
                if (failuresLeft.getAndDecrement() > 0) {
                    throw new ConcurrencyConflictException("simulated");
                }
                super.append(events, idempotency);
            }
        };
        UUID alice = open("Alice", "100.00");
        LedgerCommandService flakyService = new LedgerCommandService(flaky, Clock.systemUTC());

        flakyService.deposit(alice, new BigDecimal("1.00"), "retry", null);

        assertThat(balance(alice)).isEqualTo(10_100);
    }

    @Test
    void givesUpAfterMaxAttemptsWithConflictError() {
        EventStore alwaysConflicts = new DelegatingStore(store) {
            @Override
            public void append(List<AccountEvent> events, IdempotencyRecord idempotency) {
                throw new ConcurrencyConflictException("simulated");
            }
        };
        UUID alice = open("Alice", "100.00");
        LedgerCommandService s = new LedgerCommandService(alwaysConflicts, Clock.systemUTC());

        assertThatThrownBy(() -> s.deposit(alice, BigDecimal.ONE, "x", null))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.CONCURRENT_MODIFICATION);
    }

    @Test
    void concurrentWithdrawalsNeverOverdrawTheAccount() throws Exception {
        UUID alice = open("Alice", "100.00");
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            results.add(pool.submit(() -> {
                start.await();
                try {
                    service.withdraw(alice, new BigDecimal("10.00"), "race", null);
                    return true;
                } catch (DomainException e) {
                    return false;
                }
            }));
        }
        start.countDown();
        long succeeded = 0;
        for (Future<Boolean> f : results) {
            if (f.get(10, TimeUnit.SECONDS)) {
                succeeded++;
            }
        }
        pool.shutdown();

        assertThat(balance(alice)).isGreaterThanOrEqualTo(0);
        assertThat(balance(alice)).isEqualTo(10_000 - succeeded * 1_000);
    }

    /** Wraps a store so a test can make one method fail on purpose. */
    private static class DelegatingStore implements EventStore {
        private final EventStore delegate;

        DelegatingStore(EventStore delegate) {
            this.delegate = delegate;
        }

        public List<AccountEvent> load(UUID id) {
            return delegate.load(id);
        }

        public void append(List<AccountEvent> events, IdempotencyRecord idempotency) {
            delegate.append(events, idempotency);
        }

        public Optional<IdempotencyRecord> findIdempotency(String key) {
            return delegate.findIdempotency(key);
        }

        public List<StoredEvent> fetchUnpublished(int limit) {
            return delegate.fetchUnpublished(limit);
        }

        public void markPublished(List<Long> sequences) {
            delegate.markPublished(sequences);
        }
    }
}

package com.ledger.command.application;

import com.ledger.command.domain.Account;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.AccountEvent.TransferSent;
import com.ledger.command.domain.DomainException;
import com.ledger.command.domain.ErrorCode;
import com.ledger.command.domain.Money;
import com.ledger.command.store.ConcurrencyConflictException;
import com.ledger.command.store.DuplicateCommandException;
import com.ledger.command.store.EventStore;
import com.ledger.command.store.EventStore.IdempotencyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Handles every command on the write side. For each one I:
 * 1. return the old result if this Idempotency-Key was already used,
 * 2. load the account(s) from their events,
 * 3. let the account check the rules and create the new event(s),
 * 4. save the events together, and retry if another request saved first.
 */
public class LedgerCommandService {

    private static final Logger log = LoggerFactory.getLogger(LedgerCommandService.class);
    static final int MAX_ATTEMPTS = 6;
    static final long BASE_BACKOFF_MS = 5;

    private final EventStore store;
    private final Clock clock;

    public LedgerCommandService(EventStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    public UUID openAccount(String owner, String currency, BigDecimal initialDeposit, String idempotencyKey) {
        String fingerprint = fingerprint("open", owner, currency, String.valueOf(initialDeposit));
        return idempotent(idempotencyKey, fingerprint, () -> {
            UUID accountId = UUID.randomUUID();
            List<AccountEvent> events = new ArrayList<>();
            AccountEvent.AccountOpened opened = Account.open(accountId, owner, currency, clock);
            events.add(opened);
            if (initialDeposit != null && initialDeposit.signum() > 0) {
                Account account = Account.empty(accountId).apply(opened);
                events.add(account.deposit(Money.of(initialDeposit, currency), "initial deposit", clock));
            }
            store.append(events, record(idempotencyKey, fingerprint, accountId));
            log.info("Opened account accountId={} currency={}", accountId, currency);
            return accountId;
        });
    }

    public UUID deposit(UUID accountId, BigDecimal amount, String reference, String idempotencyKey) {
        String fingerprint = fingerprint("deposit", accountId.toString(), String.valueOf(amount), reference);
        return idempotent(idempotencyKey, fingerprint, () -> withRetry(() -> {
            Account account = load(accountId);
            AccountEvent event = account.deposit(Money.of(amount, account.currency()), reference, clock);
            store.append(List.of(event), record(idempotencyKey, fingerprint, event.eventId()));
            log.info("Deposit accountId={} amountCents={}", accountId, ((AccountEvent.FundsDeposited) event).amountCents());
            return event.eventId();
        }));
    }

    public UUID withdraw(UUID accountId, BigDecimal amount, String reference, String idempotencyKey) {
        String fingerprint = fingerprint("withdraw", accountId.toString(), String.valueOf(amount), reference);
        return idempotent(idempotencyKey, fingerprint, () -> withRetry(() -> {
            Account account = load(accountId);
            AccountEvent event = account.withdraw(Money.of(amount, account.currency()), reference, clock);
            store.append(List.of(event), record(idempotencyKey, fingerprint, event.eventId()));
            log.info("Withdrawal accountId={} amountCents={}", accountId, ((AccountEvent.FundsWithdrawn) event).amountCents());
            return event.eventId();
        }));
    }

    /**
     * I save the debit and the credit in one transaction, so a transfer is never half done.
     * Both accounts are in the same database, so I don't need a saga here.
     */
    public UUID transfer(UUID fromId, UUID toId, BigDecimal amount, String idempotencyKey) {
        if (fromId.equals(toId)) {
            throw new DomainException(ErrorCode.SAME_ACCOUNT_TRANSFER, "Cannot transfer to the same account");
        }
        String fingerprint = fingerprint("transfer", fromId.toString(), toId.toString(), String.valueOf(amount));
        return idempotent(idempotencyKey, fingerprint, () -> withRetry(() -> {
            Account from = load(fromId);
            Account to = load(toId);
            UUID transferId = UUID.randomUUID();
            TransferSent sent = from.sendTransfer(transferId, to, Money.of(amount, from.currency()), clock);
            AccountEvent received = to.receiveTransfer(sent, clock);
            store.append(List.of(sent, received), record(idempotencyKey, fingerprint, transferId));
            log.info("Transfer transferId={} from={} to={} amountCents={}", transferId, fromId, toId, sent.amountCents());
            return transferId;
        }));
    }

    public List<AccountEvent> history(UUID accountId) {
        List<AccountEvent> events = store.load(accountId);
        if (events.isEmpty()) {
            throw new DomainException(ErrorCode.ACCOUNT_NOT_FOUND, "Account " + accountId + " does not exist");
        }
        return events;
    }

    public Account currentState(UUID accountId) {
        return Account.replay(accountId, history(accountId));
    }

    // helpers

    private Account load(UUID accountId) {
        Account account = Account.replay(accountId, store.load(accountId));
        if (!account.exists()) {
            throw new DomainException(ErrorCode.ACCOUNT_NOT_FOUND, "Account " + accountId + " does not exist");
        }
        return account;
    }

    private UUID idempotent(String key, String fingerprint, Supplier<UUID> command) {
        if (key == null || key.isBlank()) {
            return command.get();
        }
        Optional<IdempotencyRecord> previous = store.findIdempotency(key);
        if (previous.isPresent()) {
            return replayResult(previous.get(), fingerprint);
        }
        try {
            return command.get();
        } catch (DuplicateCommandException e) {
            // Another request with the same key saved first, so return its result.
            return replayResult(store.findIdempotency(key).orElseThrow(), fingerprint);
        }
    }

    private UUID replayResult(IdempotencyRecord previous, String fingerprint) {
        if (!previous.fingerprint().equals(fingerprint)) {
            throw new DomainException(ErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "Idempotency key " + previous.key() + " was already used for a different request");
        }
        log.info("Idempotent replay key={} resultId={}", previous.key(), previous.resultId());
        return previous.resultId();
    }

    /**
     * Retries when another request changed the same account first. I wait a short random
     * time that grows each try, so the requests don't keep clashing at the same moment.
     */
    private UUID withRetry(Supplier<UUID> attempt) {
        for (int i = 1; ; i++) {
            try {
                return attempt.get();
            } catch (ConcurrencyConflictException e) {
                if (i >= MAX_ATTEMPTS) {
                    throw new DomainException(ErrorCode.CONCURRENT_MODIFICATION,
                            "Account was modified concurrently, please retry");
                }
                long backoffMs = ThreadLocalRandom.current().nextLong(1, (1L << i) * BASE_BACKOFF_MS);
                log.debug("Concurrency conflict, retrying in {} ms (attempt {})", backoffMs, i + 1);
                sleep(backoffMs);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new DomainException(ErrorCode.CONCURRENT_MODIFICATION, "Interrupted while retrying");
        }
    }

    private static IdempotencyRecord record(String key, String fingerprint, UUID resultId) {
        return key == null || key.isBlank() ? null : new IdempotencyRecord(key, fingerprint, resultId);
    }

    static String fingerprint(String... parts) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] hash = sha.digest(String.join("|", parts).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}

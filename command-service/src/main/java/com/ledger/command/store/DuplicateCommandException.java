package com.ledger.command.store;

/** Another request with the same Idempotency-Key was saved first. */
public class DuplicateCommandException extends RuntimeException {

    public DuplicateCommandException(String key) {
        super("Idempotency key already used: " + key);
    }
}

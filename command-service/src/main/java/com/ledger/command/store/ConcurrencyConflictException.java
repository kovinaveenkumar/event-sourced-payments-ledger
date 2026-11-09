package com.ledger.command.store;

/** Another request saved to the same account first. Load again and retry. */
public class ConcurrencyConflictException extends RuntimeException {

    public ConcurrencyConflictException(String message) {
        super(message);
    }
}

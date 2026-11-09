package com.ledger.command.domain;

/** Reasons a command can be rejected, with the HTTP status for each. */
public enum ErrorCode {
    INVALID_REQUEST(400),
    INVALID_AMOUNT(400),
    ACCOUNT_NOT_FOUND(404),
    CONCURRENT_MODIFICATION(409),
    IDEMPOTENCY_KEY_REUSED(422),
    INSUFFICIENT_FUNDS(422),
    CURRENCY_MISMATCH(422),
    SAME_ACCOUNT_TRANSFER(422),
    LIMIT_EXCEEDED(422);

    private final int httpStatus;

    ErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}

package com.ledger.command.domain;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;

/**
 * I keep money as whole cents (a long) so there are no rounding errors.
 * The API takes amounts like 150.25 and this class turns them into cents and checks them.
 */
public record Money(long cents, String currency) {

    public Money {
        if (cents <= 0) {
            throw new DomainException(ErrorCode.INVALID_AMOUNT, "Amount must be greater than zero");
        }
        validateCurrency(currency);
    }

    public static Money of(BigDecimal amount, String currency) {
        if (amount == null) {
            throw new DomainException(ErrorCode.INVALID_AMOUNT, "Amount is required");
        }
        if (amount.stripTrailingZeros().scale() > 2) {
            throw new DomainException(ErrorCode.INVALID_AMOUNT, "Amount cannot have more than 2 decimal places");
        }
        try {
            long cents = amount.setScale(2, RoundingMode.UNNECESSARY).movePointRight(2).longValueExact();
            return new Money(cents, currency);
        } catch (ArithmeticException e) {
            throw new DomainException(ErrorCode.INVALID_AMOUNT, "Amount is out of range");
        }
    }

    public static String validateCurrency(String currency) {
        try {
            if (currency == null || !Currency.getInstance(currency).getCurrencyCode().equals(currency)) {
                throw new IllegalArgumentException();
            }
            return currency;
        } catch (IllegalArgumentException e) {
            throw new DomainException(ErrorCode.INVALID_REQUEST, "Unknown ISO-4217 currency: " + currency);
        }
    }

    public static BigDecimal toDecimal(long cents) {
        return BigDecimal.valueOf(cents, 2);
    }
}

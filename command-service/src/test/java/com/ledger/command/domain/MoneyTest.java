package com.ledger.command.domain;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MoneyTest {

    @ParameterizedTest
    @CsvSource({"0.01,1", "1,100", "150.25,15025", "150.50,15050", "99999.99,9999999", "10.000,1000"})
    void convertsDecimalAmountsToCents(String amount, long cents) {
        assertThat(Money.of(new BigDecimal(amount), "USD").cents()).isEqualTo(cents);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-5.00", "0.001", "1.999"})
    void rejectsZeroNegativeAndSubCentAmounts(String amount) {
        assertThatThrownBy(() -> Money.of(new BigDecimal(amount), "USD"))
                .extracting(e -> ((DomainException) e).code())
                .isEqualTo(ErrorCode.INVALID_AMOUNT);
    }

    @ParameterizedTest
    @ValueSource(strings = {"usd", "US", "ABC"})
    void rejectsUnknownCurrencies(String currency) {
        assertThatThrownBy(() -> Money.of(BigDecimal.TEN, currency)).isInstanceOf(DomainException.class);
    }

    @ParameterizedTest
    @CsvSource({"1,0.01", "15025,150.25", "0,0.00"})
    void formatsCentsAsDecimal(long cents, String expected) {
        assertThat(Money.toDecimal(cents)).isEqualByComparingTo(expected);
    }
}

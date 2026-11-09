package com.ledger.command.web;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.util.UUID;

/** Request and response bodies for the command API. */
public final class Requests {

    private Requests() {
    }

    public record OpenAccountRequest(
            @NotBlank @Size(max = 120) String owner,
            @NotBlank @Size(min = 3, max = 3) String currency,
            @PositiveOrZero BigDecimal initialDeposit) {
    }

    public record MoneyRequest(
            @NotNull @DecimalMin(value = "0.01") BigDecimal amount,
            @Size(max = 140) String reference) {
    }

    public record TransferRequest(
            @NotNull UUID fromAccountId,
            @NotNull UUID toAccountId,
            @NotNull @DecimalMin(value = "0.01") BigDecimal amount) {
    }

    /** The command was saved. The dashboard shows it a moment later. */
    public record CommandAccepted(UUID id, String status) {
        static CommandAccepted of(UUID id) {
            return new CommandAccepted(id, "ACCEPTED");
        }
    }

    public record AccountStateResponse(UUID accountId, String owner, String currency, BigDecimal balance,
                                       long version) {
    }
}

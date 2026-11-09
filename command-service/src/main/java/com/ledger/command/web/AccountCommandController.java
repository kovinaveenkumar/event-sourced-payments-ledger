package com.ledger.command.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.command.application.LedgerCommandService;
import com.ledger.command.messaging.EventCodec;
import com.ledger.command.domain.Account;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.Money;
import com.ledger.command.web.Requests.AccountStateResponse;
import com.ledger.command.web.Requests.CommandAccepted;
import com.ledger.command.web.Requests.MoneyRequest;
import com.ledger.command.web.Requests.OpenAccountRequest;
import com.ledger.command.web.Requests.TransferRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Commands", description = "Write side: every call appends events to the ledger")
public class AccountCommandController {

    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    private final LedgerCommandService service;
    private final EventCodec codec;
    private final ObjectMapper mapper;

    public AccountCommandController(LedgerCommandService service, EventCodec codec, ObjectMapper mapper) {
        this.service = service;
        this.codec = codec;
        this.mapper = mapper;
    }

    @PostMapping("/accounts")
    @Operation(summary = "Open an account, optionally with an initial deposit")
    public ResponseEntity<CommandAccepted> open(
            @Valid @RequestBody OpenAccountRequest req,
            @Parameter(description = "Client-generated key; retries with the same key return the original result")
            @RequestHeader(value = IDEMPOTENCY_HEADER, required = false) String key) {
        UUID id = service.openAccount(req.owner(), req.currency(), req.initialDeposit(), key);
        return ResponseEntity.created(URI.create("/api/v1/accounts/" + id)).body(CommandAccepted.of(id));
    }

    @PostMapping("/accounts/{accountId}/deposits")
    @Operation(summary = "Deposit funds")
    public ResponseEntity<CommandAccepted> deposit(@PathVariable UUID accountId, @Valid @RequestBody MoneyRequest req,
                                                   @RequestHeader(value = IDEMPOTENCY_HEADER, required = false) String key) {
        return ResponseEntity.accepted().body(CommandAccepted.of(
                service.deposit(accountId, req.amount(), req.reference(), key)));
    }

    @PostMapping("/accounts/{accountId}/withdrawals")
    @Operation(summary = "Withdraw funds; rejected if the balance is insufficient")
    public ResponseEntity<CommandAccepted> withdraw(@PathVariable UUID accountId, @Valid @RequestBody MoneyRequest req,
                                                    @RequestHeader(value = IDEMPOTENCY_HEADER, required = false) String key) {
        return ResponseEntity.accepted().body(CommandAccepted.of(
                service.withdraw(accountId, req.amount(), req.reference(), key)));
    }

    @PostMapping("/transfers")
    @Operation(summary = "Transfer funds between two accounts in the same currency")
    public ResponseEntity<CommandAccepted> transfer(@Valid @RequestBody TransferRequest req,
                                                    @RequestHeader(value = IDEMPOTENCY_HEADER, required = false) String key) {
        return ResponseEntity.accepted().body(CommandAccepted.of(
                service.transfer(req.fromAccountId(), req.toAccountId(), req.amount(), key)));
    }

    @GetMapping("/accounts/{accountId}/events")
    @Operation(summary = "Full event history of an account (audit trail)")
    public List<JsonNode> events(@PathVariable UUID accountId) throws JsonProcessingException {
        List<JsonNode> out = new ArrayList<>();
        for (AccountEvent e : service.history(accountId)) {
            out.add(mapper.readTree(codec.encode(e)));
        }
        return out;
    }

    @GetMapping("/accounts/{accountId}/state")
    @Operation(summary = "Account state rebuilt on demand by replaying its events")
    public AccountStateResponse state(@PathVariable UUID accountId) {
        Account a = service.currentState(accountId);
        return new AccountStateResponse(a.id(), a.owner(), a.currency(), Money.toDecimal(a.balanceCents()), a.version());
    }
}

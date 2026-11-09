package com.ledger.query;

import com.ledger.query.ReadModelRepository.AccountView;
import com.ledger.query.ReadModelRepository.FraudAlertView;
import com.ledger.query.ReadModelRepository.Summary;
import com.ledger.query.ReadModelRepository.TransactionView;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Queries", description = "Read side: eventually consistent views built from the event stream")
public class QueryController {

    private static final int MAX_LIMIT = 500;

    private final ReadModelRepository repo;

    public QueryController(ReadModelRepository repo) {
        this.repo = repo;
    }

    @GetMapping("/accounts")
    @Operation(summary = "All accounts with their current balance")
    public List<AccountView> accounts() {
        return repo.accounts();
    }

    @GetMapping("/accounts/{accountId}")
    @Operation(summary = "One account. 404 can also mean the processor has not caught up yet")
    public AccountView account(@PathVariable UUID accountId) {
        return repo.account(accountId).orElseThrow(() -> new NotFound("Account " + accountId + " not found"));
    }

    @GetMapping("/accounts/{accountId}/transactions")
    @Operation(summary = "Most recent transactions for an account, newest first")
    public List<TransactionView> transactions(@PathVariable UUID accountId,
                                              @RequestParam(defaultValue = "50") int limit) {
        return repo.transactions(accountId, clamp(limit));
    }

    @GetMapping("/alerts")
    @Operation(summary = "Most recent fraud alerts")
    public List<FraudAlertView> alerts(@RequestParam(defaultValue = "50") int limit) {
        return repo.alerts(clamp(limit));
    }

    @GetMapping("/summary")
    @Operation(summary = "Totals for the dashboard header")
    public Summary summary() {
        return repo.summary();
    }

    private static int clamp(int limit) {
        return Math.max(1, Math.min(limit, MAX_LIMIT));
    }

    static class NotFound extends RuntimeException {
        NotFound(String message) {
            super(message);
        }
    }

    @ExceptionHandler(NotFound.class)
    ProblemDetail notFound(NotFound e) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
        p.setProperty("code", "ACCOUNT_NOT_FOUND");
        return p;
    }
}

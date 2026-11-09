package com.ledger.query;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Reads the tables the processor builds. They already match what the screens need,
 * so every query here is a simple lookup.
 */
@Repository
public class ReadModelRepository {

    public record AccountView(UUID accountId, String owner, String currency, BigDecimal balance, long version,
                              long transactionCount, Instant openedAt, Instant updatedAt) {
    }

    public record TransactionView(UUID eventId, UUID accountId, String eventType, String direction, BigDecimal amount,
                                  BigDecimal balanceAfter, UUID transferId, UUID counterpartyAccountId,
                                  Instant occurredAt) {
    }

    public record FraudAlertView(UUID alertId, UUID accountId, String owner, String rule, String detail, UUID eventId,
                                 Instant occurredAt) {
    }

    public record CurrencyTotal(String currency, long accounts, BigDecimal totalBalance) {
    }

    public record Summary(long accounts, long transactions, long fraudAlerts, long deadLetters,
                          List<CurrencyTotal> balancesByCurrency) {
    }

    private static final RowMapper<AccountView> ACCOUNT = (rs, i) -> new AccountView(
            rs.getObject("account_id", UUID.class), rs.getString("owner"), rs.getString("currency"),
            cents(rs.getLong("balance_cents")), rs.getLong("version"), rs.getLong("transaction_count"),
            rs.getTimestamp("opened_at").toInstant(), rs.getTimestamp("updated_at").toInstant());

    private static final RowMapper<TransactionView> TRANSACTION = (rs, i) -> new TransactionView(
            rs.getObject("event_id", UUID.class), rs.getObject("account_id", UUID.class), rs.getString("event_type"),
            rs.getString("direction"), cents(rs.getLong("amount_cents")), cents(rs.getLong("balance_after_cents")),
            rs.getObject("transfer_id", UUID.class), rs.getObject("counterparty_account_id", UUID.class),
            rs.getTimestamp("occurred_at").toInstant());

    private final JdbcTemplate jdbc;

    public ReadModelRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public List<AccountView> accounts() {
        return jdbc.query("SELECT * FROM account_view ORDER BY owner, opened_at", ACCOUNT);
    }

    public Optional<AccountView> account(UUID id) {
        return jdbc.query("SELECT * FROM account_view WHERE account_id = ?", ACCOUNT, id).stream().findFirst();
    }

    public List<TransactionView> transactions(UUID accountId, int limit) {
        return jdbc.query("""
                SELECT * FROM account_transactions WHERE account_id = ?
                ORDER BY occurred_at DESC, event_id LIMIT ?
                """, TRANSACTION, accountId, limit);
    }

    public List<FraudAlertView> alerts(int limit) {
        return jdbc.query("""
                SELECT f.alert_id, f.account_id, a.owner, f.rule, f.detail, f.event_id, f.occurred_at
                FROM fraud_alerts f LEFT JOIN account_view a ON a.account_id = f.account_id
                ORDER BY f.occurred_at DESC LIMIT ?
                """, (rs, i) -> new FraudAlertView(rs.getObject("alert_id", UUID.class),
                rs.getObject("account_id", UUID.class), rs.getString("owner"), rs.getString("rule"),
                rs.getString("detail"), rs.getObject("event_id", UUID.class),
                rs.getTimestamp("occurred_at").toInstant()), limit);
    }

    public Summary summary() {
        List<CurrencyTotal> totals = jdbc.query("""
                SELECT currency, COUNT(*) AS accounts, COALESCE(SUM(balance_cents), 0) AS total
                FROM account_view GROUP BY currency ORDER BY currency
                """, (rs, i) -> new CurrencyTotal(rs.getString("currency"), rs.getLong("accounts"),
                cents(rs.getLong("total"))));
        return new Summary(
                count("account_view"), count("account_transactions"), count("fraud_alerts"), count("dead_letters"),
                totals);
    }

    private long count(String table) {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Long.class);
        return n == null ? 0 : n;
    }

    private static BigDecimal cents(long cents) {
        return BigDecimal.valueOf(cents, 2);
    }
}

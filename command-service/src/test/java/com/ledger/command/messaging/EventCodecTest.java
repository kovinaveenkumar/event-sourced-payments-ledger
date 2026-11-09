package com.ledger.command.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.AccountEvent.AccountOpened;
import com.ledger.command.domain.AccountEvent.FundsDeposited;
import com.ledger.command.domain.AccountEvent.FundsWithdrawn;
import com.ledger.command.domain.AccountEvent.TransferReceived;
import com.ledger.command.domain.AccountEvent.TransferSent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventCodecTest {

    private final EventCodec codec = new EventCodec(new ObjectMapper());

    @Test
    void everyEventTypeRoundTripsThroughJson() {
        UUID acc = UUID.randomUUID();
        Instant t = Instant.parse("2026-03-01T12:00:00.123Z");
        List<AccountEvent> events = List.of(
                new AccountOpened(UUID.randomUUID(), acc, 1, t, "Alice", "USD"),
                new FundsDeposited(UUID.randomUUID(), acc, 2, t, 500, "salary"),
                new FundsWithdrawn(UUID.randomUUID(), acc, 3, t, 100, null),
                new TransferSent(UUID.randomUUID(), acc, 4, t, UUID.randomUUID(), UUID.randomUUID(), 50),
                new TransferReceived(UUID.randomUUID(), acc, 5, t, UUID.randomUUID(), UUID.randomUUID(), 25));

        for (AccountEvent e : events) {
            assertThat(codec.decode(codec.encode(e))).isEqualTo(e);
        }
    }

    @Test
    void wireFormatUsesTheDocumentedFieldNames() {
        UUID acc = UUID.randomUUID();
        String json = codec.encode(new FundsDeposited(UUID.randomUUID(), acc, 2, Instant.EPOCH, 500, "x"));
        assertThat(json).contains("\"eventType\":\"FundsDeposited\"", "\"accountId\":\"" + acc + "\"",
                "\"amountCents\":500", "\"version\":2");
    }
}

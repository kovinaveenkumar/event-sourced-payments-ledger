package com.ledger.command.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.AccountEvent.AccountOpened;
import com.ledger.command.domain.AccountEvent.FundsDeposited;
import com.ledger.command.domain.AccountEvent.FundsWithdrawn;
import com.ledger.command.domain.AccountEvent.TransferReceived;
import com.ledger.command.domain.AccountEvent.TransferSent;

import java.time.Instant;
import java.util.UUID;

/**
 * Turns events into JSON and back. The Scala processor reads this same format
 * (docs/EVENTS.md), so I wrote it by hand. That way renaming a Java field can't
 * quietly break the messages.
 */
public class EventCodec {

    private final ObjectMapper mapper;

    public EventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String encode(AccountEvent event) {
        ObjectNode n = mapper.createObjectNode();
        n.put("eventId", event.eventId().toString());
        n.put("eventType", event.type());
        n.put("accountId", event.accountId().toString());
        n.put("version", event.version());
        n.put("occurredAt", event.occurredAt().toString());
        if (event instanceof AccountOpened e) {
            n.put("owner", e.owner());
            n.put("currency", e.currency());
        } else if (event instanceof FundsDeposited e) {
            n.put("amountCents", e.amountCents());
            n.put("reference", e.reference());
        } else if (event instanceof FundsWithdrawn e) {
            n.put("amountCents", e.amountCents());
            n.put("reference", e.reference());
        } else if (event instanceof TransferSent e) {
            n.put("transferId", e.transferId().toString());
            n.put("counterpartyAccountId", e.counterpartyAccountId().toString());
            n.put("amountCents", e.amountCents());
        } else if (event instanceof TransferReceived e) {
            n.put("transferId", e.transferId().toString());
            n.put("counterpartyAccountId", e.counterpartyAccountId().toString());
            n.put("amountCents", e.amountCents());
        }
        return n.toString();
    }

    public AccountEvent decode(String json) {
        try {
            JsonNode n = mapper.readTree(json);
            UUID eventId = UUID.fromString(n.get("eventId").asText());
            UUID accountId = UUID.fromString(n.get("accountId").asText());
            long version = n.get("version").asLong();
            Instant at = Instant.parse(n.get("occurredAt").asText());
            String type = n.get("eventType").asText();
            return switch (type) {
                case "AccountOpened" -> new AccountOpened(eventId, accountId, version, at,
                        n.get("owner").asText(), n.get("currency").asText());
                case "FundsDeposited" -> new FundsDeposited(eventId, accountId, version, at,
                        n.get("amountCents").asLong(), text(n, "reference"));
                case "FundsWithdrawn" -> new FundsWithdrawn(eventId, accountId, version, at,
                        n.get("amountCents").asLong(), text(n, "reference"));
                case "TransferSent" -> new TransferSent(eventId, accountId, version, at,
                        UUID.fromString(n.get("transferId").asText()),
                        UUID.fromString(n.get("counterpartyAccountId").asText()), n.get("amountCents").asLong());
                case "TransferReceived" -> new TransferReceived(eventId, accountId, version, at,
                        UUID.fromString(n.get("transferId").asText()),
                        UUID.fromString(n.get("counterpartyAccountId").asText()), n.get("amountCents").asLong());
                default -> throw new IllegalArgumentException("Unknown event type " + type);
            };
        } catch (Exception e) {
            throw new IllegalStateException("Cannot decode event: " + json, e);
        }
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.get(field);
        return v == null || v.isNull() ? null : v.asText();
    }
}

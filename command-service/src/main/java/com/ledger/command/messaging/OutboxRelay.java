package com.ledger.command.messaging;

import com.ledger.command.store.EventStore;
import com.ledger.command.store.EventStore.StoredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Outbox pattern. Commands only save to the database. This job then sends the saved
 * events to Kafka. So I never end up with an event in the database but not in Kafka,
 * or the other way round.
 *
 * If the app stops after sending but before marking rows as sent, those events get sent
 * again. The processor skips repeats using the version number. I use the account id as
 * the Kafka key so each account's events stay in order.
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final EventStore store;
    private final KafkaTemplate<String, String> kafka;
    private final EventCodec codec;
    private final String topic;
    private final int batchSize;

    public OutboxRelay(EventStore store, KafkaTemplate<String, String> kafka, EventCodec codec,
                       String topic, int batchSize) {
        this.store = store;
        this.kafka = kafka;
        this.codec = codec;
        this.topic = topic;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.poll-interval-ms:250}")
    public void publishPending() {
        List<StoredEvent> pending = store.fetchUnpublished(batchSize);
        if (pending.isEmpty()) {
            return;
        }
        List<Long> sent = new ArrayList<>();
        try {
            for (StoredEvent stored : pending) {
                kafka.send(topic, stored.event().accountId().toString(), codec.encode(stored.event()))
                        .get(10, TimeUnit.SECONDS);
                sent.add(stored.sequence());
            }
        } catch (Exception e) {
            // Stop at the first failure so newer events don't go out before older ones.
            log.warn("Outbox publish failed after {} of {} events; will retry", sent.size(), pending.size(), e);
        } finally {
            if (!sent.isEmpty()) {
                store.markPublished(sent);
                log.info("Published {} events to topic {}", sent.size(), topic);
            }
        }
    }
}

package com.ledger.command.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.command.domain.AccountEvent;
import com.ledger.command.domain.AccountEvent.AccountOpened;
import com.ledger.command.domain.AccountEvent.FundsDeposited;
import com.ledger.command.store.InMemoryEventStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sends events through the outbox relay to a real Kafka broker in Docker and reads them
 * back, to check the key, the JSON and the order the processor will see.
 */
@Testcontainers
class OutboxRelayIT {

    private static final String TOPIC = "ledger-events-it";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    @Test
    void publishesEventsKeyedByAccountInOrderAndMarksThemSent() {
        InMemoryEventStore store = new InMemoryEventStore();
        EventCodec codec = new EventCodec(new ObjectMapper());
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        store.append(List.of(
                new AccountOpened(UUID.randomUUID(), id, 1, now, "Alice", "USD"),
                new FundsDeposited(UUID.randomUUID(), id, 2, now, 1500, "salary"),
                new FundsDeposited(UUID.randomUUID(), id, 3, now, 250, "refund")), null);

        KafkaTemplate<String, String> kafka = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all")));

        new OutboxRelay(store, kafka, codec, TOPIC, 100).publishPending();

        assertThat(store.fetchUnpublished(10)).isEmpty();

        List<ConsumerRecord<String, String>> received = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 30_000;
            while (received.size() < 3 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
            }
        }

        assertThat(received).hasSize(3);
        assertThat(received).allSatisfy(r -> assertThat(r.key()).isEqualTo(id.toString()));
        assertThat(received).extracting(r -> codec.decode(r.value()).version()).containsExactly(1L, 2L, 3L);
        AccountEvent second = codec.decode(received.get(1).value());
        assertThat(second).isInstanceOf(FundsDeposited.class);
        assertThat(((FundsDeposited) second).amountCents()).isEqualTo(1500);
    }
}

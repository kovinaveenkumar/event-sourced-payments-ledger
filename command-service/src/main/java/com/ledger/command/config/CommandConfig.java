package com.ledger.command.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.command.application.LedgerCommandService;
import com.ledger.command.messaging.EventCodec;
import com.ledger.command.messaging.OutboxRelay;
import com.ledger.command.store.EventStore;
import com.ledger.command.store.JdbcEventStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Clock;

/** Creates the beans. I kept the domain classes plain Java, so Spring is only set up here. */
@Configuration
public class CommandConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EventCodec eventCodec(ObjectMapper mapper) {
        return new EventCodec(mapper);
    }

    @Bean
    EventStore eventStore(JdbcTemplate jdbc, TransactionTemplate tx, EventCodec codec) {
        return new JdbcEventStore(jdbc, tx, codec);
    }

    @Bean
    LedgerCommandService ledgerCommandService(EventStore store, Clock clock) {
        return new LedgerCommandService(store, clock);
    }

    @Bean
    @ConditionalOnProperty(name = "ledger.outbox.enabled", havingValue = "true", matchIfMissing = true)
    OutboxRelay outboxRelay(EventStore store, KafkaTemplate<String, String> kafka, EventCodec codec,
                            @Value("${ledger.kafka.topic}") String topic,
                            @Value("${ledger.outbox.batch-size:200}") int batchSize) {
        return new OutboxRelay(store, kafka, codec, topic, batchSize);
    }

    /** Allows the dashboard on port 8082 to call this API from the browser. */
    @Bean
    WebMvcConfigurer cors(@Value("${ledger.cors.allowed-origins}") String[] origins) {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/api/**").allowedOrigins(origins).allowedMethods("GET", "POST")
                        .allowedHeaders("*");
            }
        };
    }
}

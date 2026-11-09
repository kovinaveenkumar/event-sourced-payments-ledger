package com.ledger.command.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledger.command.application.LedgerCommandService;
import com.ledger.command.messaging.EventCodec;
import com.ledger.command.store.InMemoryEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Clock;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Tests the REST endpoints, input checks and error responses using the in-memory store. */
@WebMvcTest(controllers = AccountCommandController.class,
        properties = "ledger.cors.allowed-origins=http://localhost:8082")
class AccountCommandControllerTest {

    @TestConfiguration
    static class Config {
        @Bean
        LedgerCommandService service() {
            return new LedgerCommandService(new InMemoryEventStore(), Clock.systemUTC());
        }

        @Bean
        EventCodec codec(ObjectMapper mapper) {
            return new EventCodec(mapper);
        }
    }

    @Autowired
    MockMvc mvc;
    @Autowired
    ObjectMapper mapper;

    private String openAccount(String owner, String deposit) throws Exception {
        MvcResult r = mvc.perform(post("/api/v1/accounts").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"owner\":\"" + owner + "\",\"currency\":\"USD\",\"initialDeposit\":" + deposit + "}"))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andReturn();
        JsonNode body = mapper.readTree(r.getResponse().getContentAsString());
        return body.get("id").asText();
    }

    @Test
    void openDepositAndReadReplayedState() throws Exception {
        String id = openAccount("Alice", "100.00");

        mvc.perform(post("/api/v1/accounts/" + id + "/deposits").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"amount\":25.50,\"reference\":\"salary\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        mvc.perform(get("/api/v1/accounts/" + id + "/state"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value(125.50))
                .andExpect(jsonPath("$.version").value(3));

        mvc.perform(get("/api/v1/accounts/" + id + "/events"))
                .andExpect(jsonPath("$[0].eventType").value("AccountOpened"))
                .andExpect(jsonPath("$[2].amountCents").value(2550));
    }

    @Test
    void overdraftTransferReturns422ProblemDetail() throws Exception {
        String alice = openAccount("Alice", "100.00");
        String bob = openAccount("Bob", "0");

        mvc.perform(post("/api/v1/transfers").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"fromAccountId\":\"" + alice + "\",\"toAccountId\":\"" + bob + "\",\"amount\":150}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.code").value("INSUFFICIENT_FUNDS"));
    }

    @Test
    void invalidBodyReturns400WithFieldDetails() throws Exception {
        mvc.perform(post("/api/v1/accounts").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"owner\":\"\",\"currency\":\"US\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("owner")));
    }

    @Test
    void unknownAccountReturns404() throws Exception {
        mvc.perform(get("/api/v1/accounts/" + java.util.UUID.randomUUID() + "/state"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("ACCOUNT_NOT_FOUND"));
    }

    @Test
    void idempotencyKeyReturnsTheSameTransferId() throws Exception {
        String alice = openAccount("Alice", "100.00");
        String bob = openAccount("Bob", "0");
        String body = "{\"fromAccountId\":\"" + alice + "\",\"toAccountId\":\"" + bob + "\",\"amount\":10}";

        String first = mvc.perform(post("/api/v1/transfers").header("Idempotency-Key", "abc")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andReturn().getResponse().getContentAsString();
        String second = mvc.perform(post("/api/v1/transfers").header("Idempotency-Key", "abc")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andReturn().getResponse().getContentAsString();

        org.assertj.core.api.Assertions.assertThat(second).isEqualTo(first);
        mvc.perform(get("/api/v1/accounts/" + alice + "/state")).andExpect(jsonPath("$.balance").value(90.0));
    }
}

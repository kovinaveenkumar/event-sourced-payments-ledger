package com.ledger.query;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Tests the read API end to end using an in-memory H2 database with sample data. */
@SpringBootTest
@AutoConfigureMockMvc
class QueryControllerTest {

    private static final String ALICE = "11111111-1111-1111-1111-111111111111";

    @Autowired
    MockMvc mvc;

    @Test
    void listsAccountsSortedByOwnerWithDecimalBalances() throws Exception {
        mvc.perform(get("/api/v1/accounts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(3)))
                .andExpect(jsonPath("$[0].owner").value("Alice"))
                .andExpect(jsonPath("$[0].balance").value(70.00));
    }

    @Test
    void returnsOneAccountOr404() throws Exception {
        mvc.perform(get("/api/v1/accounts/" + ALICE)).andExpect(jsonPath("$.version").value(3));
        mvc.perform(get("/api/v1/accounts/99999999-9999-9999-9999-999999999999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("ACCOUNT_NOT_FOUND"));
    }

    @Test
    void returnsTransactionsNewestFirstAndHonoursLimit() throws Exception {
        mvc.perform(get("/api/v1/accounts/" + ALICE + "/transactions"))
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].eventType").value("TransferSent"))
                .andExpect(jsonPath("$[0].balanceAfter").value(70.00));
        mvc.perform(get("/api/v1/accounts/" + ALICE + "/transactions?limit=1")).andExpect(jsonPath("$", hasSize(1)));
    }

    @Test
    void alertsIncludeTheAccountOwner() throws Exception {
        mvc.perform(get("/api/v1/alerts"))
                .andExpect(jsonPath("$[0].rule").value("VELOCITY"))
                .andExpect(jsonPath("$[0].owner").value("Alice"));
    }

    @Test
    void summaryGroupsBalancesByCurrency() throws Exception {
        mvc.perform(get("/api/v1/summary"))
                .andExpect(jsonPath("$.accounts").value(3))
                .andExpect(jsonPath("$.fraudAlerts").value(1))
                .andExpect(jsonPath("$.balancesByCurrency[0].currency").value("EUR"))
                .andExpect(jsonPath("$.balancesByCurrency[1].totalBalance").value(120.00));
    }

    @Test
    void servesTheDashboard() throws Exception {
        mvc.perform(get("/index.html")).andExpect(status().isOk());
    }
}

package com.assignment.coupon_system.issuerequest.controller;

import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.coupon.redis.CouponReservation;
import com.assignment.coupon_system.issuerequest.entity.CouponIssueResult;
import com.assignment.coupon_system.issuerequest.repository.CouponIssueResultRepository;
import com.assignment.coupon_system.issuerequest.service.CouponIssueQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.Optional;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CouponIssueQueryControllerTest {
    private static final String ID = "82a742c0-6cb5-4dc4-8cf8-d13b42514e17";
    private static final String URL = "/api/coupons/1/issue-requests/" + ID;
    private CouponIssueResultRepository repository;
    private CouponIssueRedisService redis;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        repository = mock(CouponIssueResultRepository.class);
        redis = mock(CouponIssueRedisService.class);
        mvc = MockMvcBuilders.standaloneSetup(new CouponIssueQueryController(
                new CouponIssueQueryService(repository, redis))).build();
    }

    @Test
    void issuedResultDoesNotDependOnRedis() throws Exception {
        when(repository.findByRequestId(ID)).thenReturn(Optional.of(
                CouponIssueResult.issued(ID, 1L, 100L, 50L)));
        mvc.perform(get(URL)).andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.requestId").value(ID))
                .andExpect(jsonPath("$.data.couponId").value(1))
                .andExpect(jsonPath("$.data.userId").value(100))
                .andExpect(jsonPath("$.data.status").value("ISSUED"))
                .andExpect(jsonPath("$.data.issuedCouponId").value(50));
        verifyNoInteractions(redis);
    }

    @Test
    void reservationIsReturnedOnlyAfterDbMiss() throws Exception {
        when(redis.findReservationIfPresent(ID)).thenReturn(Optional.of(
                new CouponReservation(ID, 1L, 100L, Instant.now(), 1)));
        mvc.perform(get(URL)).andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("RESERVED"))
                .andExpect(jsonPath("$.data.issuedCouponId").isEmpty());
        var order = inOrder(repository, redis);
        order.verify(repository).findByRequestId(ID);
        order.verify(redis).findReservationIfPresent(ID);
        verifyNoMoreInteractions(repository, redis);
    }

    @Test
    void absentInBothStoresReturns404() throws Exception {
        mvc.perform(get(URL)).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.success").value(false));
    }

    @Test
    void dbCouponMismatchDoesNotFallBackToRedis() throws Exception {
        when(repository.findByRequestId(ID)).thenReturn(Optional.of(
                CouponIssueResult.issued(ID, 2L, 100L, 50L)));
        mvc.perform(get(URL)).andExpect(status().isNotFound());
        verifyNoInteractions(redis);
    }

    @Test
    void redisCouponMismatchReturns404() throws Exception {
        when(redis.findReservationIfPresent(ID)).thenReturn(Optional.of(
                new CouponReservation(ID, 2L, 100L, Instant.now(), 1)));
        mvc.perform(get(URL)).andExpect(status().isNotFound());
    }

    @Test
    void dbErrorNeverFallsBackToRedis() throws Exception {
        when(repository.findByRequestId(ID)).thenThrow(new QueryTimeoutException("db timeout"));
        expectUnavailable();
        verifyNoInteractions(redis);
        verify(repository).findByRequestId(ID);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void redisErrorIsNotNotFoundOrRejected() throws Exception {
        when(redis.findReservationIfPresent(ID)).thenThrow(new QueryTimeoutException("redis timeout"));
        expectUnavailable();
        verify(redis).findReservationIfPresent(ID);
        verify(repository).findByRequestId(ID);
        verifyNoMoreInteractions(repository, redis);
    }

    @Test
    void damagedReservationReturns503() throws Exception {
        when(redis.findReservationIfPresent(ID)).thenThrow(new IllegalStateException("damaged hash"));
        expectUnavailable();
    }

    @Test
    void returnsLegacyStoredRejectionWithoutInventingNewResult() throws Exception {
        when(repository.findByRequestId(ID)).thenReturn(Optional.of(
                CouponIssueResult.rejected(ID, 1L, 100L)));
        mvc.perform(get(URL)).andExpect(status().isOk())
                .andExpect(jsonPath("$.data.status").value("REJECTED"))
                .andExpect(jsonPath("$.data.issuedCouponId").isEmpty());
        verifyNoInteractions(redis);
        verify(repository).findByRequestId(ID);
        verifyNoMoreInteractions(repository);
    }

    @Test
    void subsequentLookupSeesCompletedIssuance() throws Exception {
        when(repository.findByRequestId(ID)).thenReturn(Optional.empty())
                .thenReturn(Optional.of(CouponIssueResult.issued(ID, 1L, 100L, 50L)));
        when(redis.findReservationIfPresent(ID)).thenReturn(Optional.of(
                new CouponReservation(ID, 1L, 100L, Instant.now(), 1)));
        mvc.perform(get(URL)).andExpect(jsonPath("$.data.status").value("RESERVED"));
        mvc.perform(get(URL)).andExpect(jsonPath("$.data.status").value("ISSUED"));
        verify(redis, times(1)).findReservationIfPresent(ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "not-a-number"})
    void invalidCouponDoesNotReadStores(String couponId) throws Exception {
        mvc.perform(get("/api/coupons/" + couponId + "/issue-requests/" + ID))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(repository, redis);
    }

    @Test
    void invalidUuidDoesNotReadStores() throws Exception {
        mvc.perform(get("/api/coupons/1/issue-requests/invalid"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(repository, redis);
    }

    private void expectUnavailable() throws Exception {
        mvc.perform(get(URL)).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.data").isEmpty())
                .andExpect(jsonPath("$.message").value(containsString("같은 requestId")));
    }
}

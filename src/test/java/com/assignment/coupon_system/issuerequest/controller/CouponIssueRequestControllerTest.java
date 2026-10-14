package com.assignment.coupon_system.issuerequest.controller;

import com.assignment.coupon_system.coupon.redis.CouponIssueRedisService;
import com.assignment.coupon_system.coupon.redis.CouponReservation;
import com.assignment.coupon_system.coupon.redis.CouponReservationStatus;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CouponIssueRequestControllerTest {
    private static final String ID = "82a742c0-6cb5-4dc4-8cf8-d13b42514e17";
    private static final String BODY = "{\"requestId\":\"" + ID + "\",\"userId\":100}";
    private CouponIssueRedisService redis;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        redis = mock(CouponIssueRedisService.class);
        mvc = MockMvcBuilders.standaloneSetup(new CouponIssueRequestController(redis))
                .setMessageConverters(new MappingJackson2HttpMessageConverter(
                        Jackson2ObjectMapperBuilder.json()
                                .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                                .build()))
                .build();
    }

    @ParameterizedTest
    @EnumSource(value = CouponReservationStatus.class, names = {"RESERVED", "ALREADY_RESERVED"})
    void returnsStoredApproval(CouponReservationStatus status) throws Exception {
        when(redis.reserve(ID, 1L, 100L)).thenReturn(status);
        when(redis.findReservation(ID)).thenReturn(new CouponReservation(
                ID, 1L, 100L, Instant.parse("2026-10-08T05:00:00Z"), 1));

        mvc.perform(post("/api/coupons/1/issue-requests")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.requestId").value(ID))
                .andExpect(jsonPath("$.data.couponId").value(1))
                .andExpect(jsonPath("$.data.userId").value(100))
                .andExpect(jsonPath("$.data.status").value("RESERVED"))
                .andExpect(jsonPath("$.data.reservedAt").value("2026-10-08T05:00:00Z"))
                .andExpect(jsonPath("$.data.reserveAt").doesNotExist());
        verify(redis).reserve(ID, 1L, 100L);
        verify(redis).findReservation(ID);
        verifyNoMoreInteractions(redis);
    }

    @ParameterizedTest
    @EnumSource(value = CouponReservationStatus.class,
            names = {"SOLD_OUT", "DUPLICATE_USER", "REQUEST_CONFLICT", "NOT_AVAILABLE"})
    void distinguishesBusinessRejections(CouponReservationStatus result) throws Exception {
        when(redis.reserve(ID, 1L, 100L)).thenReturn(result);
        mvc.perform(post("/api/coupons/1/issue-requests")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value(containsString(result.name())));
        verify(redis).reserve(ID, 1L, 100L);
        verifyNoMoreInteractions(redis);
    }

    @Test
    void reservationTimeoutDoesNotReportRejectionOrRollback() throws Exception {
        when(redis.reserve(ID, 1L, 100L)).thenThrow(new QueryTimeoutException("timeout"));
        expectUnavailable();
        verify(redis).reserve(ID, 1L, 100L);
        verifyNoMoreInteractions(redis);
    }

    @Test
    void lookupTimeoutAfterApprovalDoesNotUndoReservation() throws Exception {
        when(redis.reserve(ID, 1L, 100L)).thenReturn(CouponReservationStatus.RESERVED);
        when(redis.findReservation(ID)).thenThrow(new QueryTimeoutException("timeout"));
        expectUnavailable();
        verify(redis).reserve(ID, 1L, 100L);
        verify(redis).findReservation(ID);
        verifyNoMoreInteractions(redis);
    }

    @Test
    void missingInitializationReturnsUnavailable() throws Exception {
        when(redis.reserve(ID, 1L, 100L)).thenThrow(new IllegalStateException("policy missing"));
        expectUnavailable();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"userId\":100}",
            "{\"requestId\":\"invalid\",\"userId\":100}",
            "{\"requestId\":\"82a742c0-6cb5-4dc4-8cf8-d13b42514e17\",\"userId\":0}",
            "{\"requestId\":\"82a742c0-6cb5-4dc4-8cf8-d13b42514e17\"}"})
    void invalidBodyDoesNotCallRedis(String body) throws Exception {
        mvc.perform(post("/api/coupons/1/issue-requests")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(redis);
    }

    @Test
    void invalidCouponDoesNotCallRedis() throws Exception {
        mvc.perform(post("/api/coupons/0/issue-requests")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(redis);
    }

    private void expectUnavailable() throws Exception {
        mvc.perform(post("/api/coupons/1/issue-requests")
                        .contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.message").value(containsString("같은 requestId")));
    }
}

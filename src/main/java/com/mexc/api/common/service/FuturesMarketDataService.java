package com.mexc.api.common.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.mexc.api.common.ApiClient;
import com.mexc.api.common.constant.URI;
import com.mexc.api.common.dto.ResponseResult;

import java.util.Map;

public class FuturesMarketDataService {
    private final ApiClient apiClient;

    public FuturesMarketDataService(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public ResponseResult getExchangeInfo() {
        return apiClient.get(URI.CONTRACT_INFORMATION, Maps.newHashMap(), new TypeReference<ResponseResult>() {});
    }

    public ResponseResult getCurrentFundingRate() {
        return apiClient.get(URI.CONTRACT_TREND_DATA, Maps.newHashMap(), new TypeReference<ResponseResult>() {});
    }
}

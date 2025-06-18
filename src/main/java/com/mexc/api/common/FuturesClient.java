package com.mexc.api.common;

import com.mexc.api.common.dto.ResponseResult;
import com.mexc.api.common.service.FuturesMarketDataService;
import lombok.Getter;

@Getter
public class FuturesClient {
    private final ApiClient apiClient;
    private final String BASE_URL = "https://contract.mexc.com/";
    private final FuturesMarketDataService marketDataService;

    public FuturesClient(String apiKey, String secretKey) {
        this.apiClient = new ApiClient(BASE_URL, apiKey, secretKey);
        this.marketDataService = new FuturesMarketDataService(apiClient);
    }

    public FuturesMarketDataService market() {
        return this.marketDataService;
    }


}

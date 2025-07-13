package com.mexc.api.common.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MEXC REST API Helper Class
 *
 * This class provides REST API functionality to support the WebSocket streaming client,
 * particularly for managing listen keys and other authenticated operations.
 */
public class MexcRestApiHelper {

    private static final Logger LOGGER = Logger.getLogger(MexcRestApiHelper.class.getName());

    private static final String SPOT_API_BASE = "https://api.mexc.com";
    private static final String FUTURES_API_BASE = "https://contract.mexc.com";

    private final String apiKey;
    private final String secretKey;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public MexcRestApiHelper(String apiKey, String secretKey) {
        this.apiKey = apiKey;
        this.secretKey = secretKey;
        this.httpClient = HttpClients.createDefault();
        this.objectMapper = new ObjectMapper();
    }

    // ================== SPOT API METHODS ==================

    /**
     * Create a new spot listen key for user data streams
     */
    public CompletableFuture<String> createSpotListenKey() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long timestamp = Instant.now().toEpochMilli();
                String queryString = "timestamp=" + timestamp;
                String signature = createHmacSha256(queryString, secretKey);

                String url = SPOT_API_BASE + "/api/v3/userDataStream?" + queryString + "&signature=" + signature;
                HttpPost request = new HttpPost(url);
                request.setHeader("X-MEXC-APIKEY", apiKey);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                JsonNode json = objectMapper.readTree(response);

                if (json.has("listenKey")) {
                    String listenKey = json.get("listenKey").asText();
                    LOGGER.info("Created new spot listen key: " + listenKey.substring(0, 8) + "...");
                    return listenKey;
                } else {
                    throw new RuntimeException("Failed to create listen key: " + response);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error creating spot listen key", e);
            }
        });
    }

    /**
     * Extend the validity of an existing spot listen key
     */
    public CompletableFuture<Void> extendSpotListenKey(String listenKey) {
        return CompletableFuture.runAsync(() -> {
            try {
                long timestamp = Instant.now().toEpochMilli();
                String queryString = "listenKey=" + URLEncoder.encode(listenKey, StandardCharsets.UTF_8) +
                        "&timestamp=" + timestamp;
                String signature = createHmacSha256(queryString, secretKey);

                String url = SPOT_API_BASE + "/api/v3/userDataStream?" + queryString + "&signature=" + signature;
                HttpPut request = new HttpPut(url);
                request.setHeader("X-MEXC-APIKEY", apiKey);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                LOGGER.info("Extended spot listen key validity");
            } catch (Exception e) {
                throw new RuntimeException("Error extending spot listen key", e);
            }
        });
    }

    /**
     * Delete/close a spot listen key
     */
    public CompletableFuture<Void> deleteSpotListenKey(String listenKey) {
        return CompletableFuture.runAsync(() -> {
            try {
                long timestamp = Instant.now().toEpochMilli();
                String queryString = "listenKey=" + URLEncoder.encode(listenKey, StandardCharsets.UTF_8) +
                        "&timestamp=" + timestamp;
                String signature = createHmacSha256(queryString, secretKey);

                String url = SPOT_API_BASE + "/api/v3/userDataStream?" + queryString + "&signature=" + signature;
                HttpDelete request = new HttpDelete(url);
                request.setHeader("X-MEXC-APIKEY", apiKey);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                LOGGER.info("Deleted spot listen key");
            } catch (Exception e) {
                throw new RuntimeException("Error deleting spot listen key", e);
            }
        });
    }

    /**
     * Get all valid spot listen keys
     */
    public CompletableFuture<JsonNode> getSpotListenKeys() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long timestamp = Instant.now().toEpochMilli();
                String queryString = "timestamp=" + timestamp;
                String signature = createHmacSha256(queryString, secretKey);

                String url = SPOT_API_BASE + "/api/v3/userDataStream?" + queryString + "&signature=" + signature;
                HttpGet request = new HttpGet(url);
                request.setHeader("X-MEXC-APIKEY", apiKey);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting spot listen keys", e);
            }
        });
    }

    // ================== FUTURES API METHODS ==================

    /**
     * Get futures account information
     */
    public CompletableFuture<JsonNode> getFuturesAccount() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long timestamp = Instant.now().toEpochMilli();
                String requestParam = "";
                String signatureData = apiKey + timestamp + requestParam;
                String signature = createHmacSha256(signatureData, secretKey);

                HttpGet request = new HttpGet(FUTURES_API_BASE + "/api/v1/private/account/assets");
                request.setHeader("ApiKey", apiKey);
                request.setHeader("Request-Time", String.valueOf(timestamp));
                request.setHeader("Signature", signature);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting futures account", e);
            }
        });
    }

    /**
     * Get futures positions
     */
    public CompletableFuture<JsonNode> getFuturesPositions() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                long timestamp = Instant.now().toEpochMilli();
                String requestParam = "";
                String signatureData = apiKey + timestamp + requestParam;
                String signature = createHmacSha256(signatureData, secretKey);

                HttpGet request = new HttpGet(FUTURES_API_BASE + "/api/v1/private/position/open_positions");
                request.setHeader("ApiKey", apiKey);
                request.setHeader("Request-Time", String.valueOf(timestamp));
                request.setHeader("Signature", signature);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting futures positions", e);
            }
        });
    }

    /**
     * Get futures open orders
     */
    public CompletableFuture<JsonNode> getFuturesOpenOrders(String symbol) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String endpoint = "/api/v1/private/order/list/open_orders";
                if (symbol != null && !symbol.isEmpty()) {
                    endpoint += "/" + symbol;
                }

                long timestamp = Instant.now().toEpochMilli();
                String requestParam = "page_num=1&page_size=100";
                String signatureData = apiKey + timestamp + requestParam;
                String signature = createHmacSha256(signatureData, secretKey);

                String url = FUTURES_API_BASE + endpoint + "?" + requestParam;
                HttpGet request = new HttpGet(url);
                request.setHeader("ApiKey", apiKey);
                request.setHeader("Request-Time", String.valueOf(timestamp));
                request.setHeader("Signature", signature);
                request.setHeader("Content-Type", "application/json");

                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting futures open orders", e);
            }
        });
    }

    // ================== MARKET DATA METHODS ==================

    /**
     * Get spot exchange information
     */
    public CompletableFuture<JsonNode> getSpotExchangeInfo() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                HttpGet request = new HttpGet(SPOT_API_BASE + "/api/v3/exchangeInfo");
                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting spot exchange info", e);
            }
        });
    }

    /**
     * Get futures contract information
     */
    public CompletableFuture<JsonNode> getFuturesContractInfo() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                HttpGet request = new HttpGet(FUTURES_API_BASE + "/api/v1/contract/detail");
                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting futures contract info", e);
            }
        });
    }

    /**
     * Get spot order book
     */
    public CompletableFuture<JsonNode> getSpotOrderBook(String symbol, Integer limit) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String params = "symbol=" + symbol;
                if (limit != null) {
                    params += "&limit=" + limit;
                }

                HttpGet request = new HttpGet(SPOT_API_BASE + "/api/v3/depth?" + params);
                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting spot order book", e);
            }
        });
    }

    /**
     * Get futures order book
     */
    public CompletableFuture<JsonNode> getFuturesOrderBook(String symbol, Integer limit) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String endpoint = "/api/v1/contract/depth/" + symbol;
                if (limit != null) {
                    endpoint += "?limit=" + limit;
                }

                HttpGet request = new HttpGet(FUTURES_API_BASE + endpoint);
                String response = executeRequest(request);
                return objectMapper.readTree(response);
            } catch (Exception e) {
                throw new RuntimeException("Error getting futures order book", e);
            }
        });
    }

    // ================== AUTHENTICATION HELPERS ==================

    private String createHmacSha256(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create HMAC SHA256 signature", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    // ================== HTTP CLIENT HELPERS ==================

    private String executeRequest(HttpUriRequestBase request) throws IOException {
        try (var response = httpClient.execute(request)) {
            int statusCode = response.getCode();
            String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

            if (statusCode >= 200 && statusCode < 300) {
                return responseBody;
            } else {
                throw new IOException("HTTP " + statusCode + ": " + responseBody);
            }
        }
    }

    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error closing HTTP client", e);
        }
    }
}


package com.mexc.api.common.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.mexc.api.common.MexcFuturesStreamClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RobustMexcClient {
    private static final Logger LOGGER = Logger.getLogger(RobustMexcClient.class.getName());

    private final MexcFuturesStreamClient streamClient;
    private final MexcRestApiHelper restApi;
    private final ListenKeyManager listenKeyManager;
    private final ScheduledExecutorService healthChecker = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean isHealthy = true;
    private final Map<String, Long> lastDataReceived = new ConcurrentHashMap<>();

    public RobustMexcClient(String apiKey, String secretKey) {
        this.restApi = new MexcRestApiHelper(apiKey, secretKey);
        this.listenKeyManager = new ListenKeyManager(restApi);
        this.streamClient = new MexcFuturesStreamClient(apiKey, secretKey);

        setupErrorHandling();
        startHealthMonitoring();
    }

    private void setupErrorHandling() {
        streamClient.setOnError(error -> {
            LOGGER.log(Level.SEVERE, "WebSocket error occurred", error);
            isHealthy = false;

            // Attempt recovery based on error type
            if (error.getMessage().contains("authentication") ||
                    error.getMessage().contains("listen key")) {

                LOGGER.info("Authentication error detected, refreshing listen key...");
                listenKeyManager.refresh()
                        .thenRun(() -> {
                            LOGGER.info("Listen key refreshed, reconnecting...");
                            isHealthy = true;
                        })
                        .exceptionally(ex -> {
                            LOGGER.log(Level.SEVERE, "Failed to refresh listen key", ex);
                            return null;
                        });
            }
        });

        streamClient.setOnDisconnect(connectionId -> {
            LOGGER.warning("Connection lost: " + connectionId);
            lastDataReceived.remove(connectionId);
        });

        streamClient.setOnConnect(connectionId -> {
            LOGGER.info("Connection established: " + connectionId);
            lastDataReceived.put(connectionId, System.currentTimeMillis());
            isHealthy = true;
        });
    }

    private void startHealthMonitoring() {
        healthChecker.scheduleAtFixedRate(() -> {
            checkConnectionHealth();
            checkDataFlow();
        }, 60, 60, TimeUnit.SECONDS);
    }

    private void checkConnectionHealth() {
        boolean spotConnected = streamClient.isSpotConnected();
        boolean futuresConnected = streamClient.isFuturesConnected();

        if (!spotConnected || !futuresConnected) {
            LOGGER.warning("Connection health check failed - Spot: " + spotConnected +
                    ", Futures: " + futuresConnected);
            isHealthy = false;
        }
    }

    private void checkDataFlow() {
        long now = System.currentTimeMillis();

        for (Map.Entry<String, Long> entry : lastDataReceived.entrySet()) {
            String connectionId = entry.getKey();
            long lastActivity = entry.getValue();
            long timeSinceLastData = now - lastActivity;

            if (timeSinceLastData > 300000) { // No data for 5 minutes
                LOGGER.warning("No data received from " + connectionId +
                        " for " + (timeSinceLastData / 1000) + " seconds");
                isHealthy = false;
            }
        }
    }

    public boolean isHealthy() {
        return isHealthy;
    }

    public void shutdown() {
        healthChecker.shutdown();
        streamClient.closeAll();
        listenKeyManager.shutdown();
        restApi.close();
    }

    // Delegate methods to underlying clients...
    public void subscribeSpotTicker(String symbol, Consumer<JsonNode> handler) {
        streamClient.subscribeSpotTicker(symbol, data -> {
            lastDataReceived.put("spot", System.currentTimeMillis());
            handler.accept(data);
        });
    }

    public void subscribeFuturesTicker(String symbol, Consumer<JsonNode> handler) {
        streamClient.subscribeFuturesTicker(symbol, data -> {
            lastDataReceived.put("futures", System.currentTimeMillis());
            handler.accept(data);
        });
    }

    // Add more delegation methods as needed...
}

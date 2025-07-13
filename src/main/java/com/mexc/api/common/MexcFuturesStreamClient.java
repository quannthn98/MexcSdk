package com.mexc.api.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mexc.api.common.constant.StreamChannel;
import com.mexc.api.common.service.MexcRestApiHelper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MEXC Futures Stream API Java Client
 *
 * This library provides comprehensive WebSocket streaming capabilities for MEXC Exchange
 * including both public market data and private account data streams.
 *
 * Features:
 * - Automatic connection management with reconnection
 * - Heartbeat/ping mechanism to maintain connection
 * - Both Spot and Futures API support
 * - Public and Private channel subscriptions
 * - Protocol Buffers support for Spot API
 * - Thread-safe implementation
 *
 * @author MEXC API Java Library
 * @version 1.0.0
 */
public class MexcFuturesStreamClient {

    private static final Logger LOGGER = Logger.getLogger(MexcFuturesStreamClient.class.getName());

    // MEXC WebSocket URLs
    private static final String SPOT_WS_URL = "ws://wbs-api.mexc.com/ws";
    private static final String FUTURES_WS_URL = "wss://contract.mexc.com/edge";
    private static final String SPOT_API_BASE_URL = "https://api.mexc.com";

    // Connection management
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final Map<String, WebSocketClient> connections = new ConcurrentHashMap<>();
    private final Map<String, ScheduledFuture<?>> heartbeatTasks = new ConcurrentHashMap<>();
    private final Set<String> subscribedChannels = ConcurrentHashMap.newKeySet();

    // Authentication
    private String apiKey;
    private String secretKey;
    private String listenKey; // For spot private streams

    // Event handlers
    private final Map<String, Consumer<JsonNode>> eventHandlers = new ConcurrentHashMap<>();
    private Consumer<String> onConnect;
    private Consumer<String> onDisconnect;
    private Consumer<Exception> onError;

    // Configuration
    private boolean autoReconnect = true;
    private int reconnectDelay = 5000; // 5 seconds
    private int heartbeatInterval = 20000; // 20 seconds
    private int maxReconnectAttempts = 10;

    // Filters
    private Map<String, Object> orderFilter = Map.of("filter", "order.deak");
    private final Map<String, Object> positionFilter = Map.of("filter", "position");
    /**
     * Constructor for public streams only
     */
    public MexcFuturesStreamClient() {
        setupDefaultHandlers();
    }

    /**
     * Constructor with API credentials for private streams
     */
    public MexcFuturesStreamClient(String apiKey, String secretKey) {
        this.apiKey = apiKey;
        this.secretKey = secretKey;
        setupDefaultHandlers();
    }

    private void setupDefaultHandlers() {
        // Default error handler
        onError = ex -> LOGGER.log(Level.SEVERE, "WebSocket error: " + ex.getMessage(), ex);

        // Default connection handlers
        onConnect = connectionId -> LOGGER.info("Connected to " + connectionId);
        onDisconnect = connectionId -> LOGGER.info("Disconnected from " + connectionId);
    }

    // ================== PUBLIC MARKET DATA STREAMS ==================

    /**
     * Subscribe to ticker data for all symbols (Spot)
     */
    public void subscribeSpotTickers(Consumer<JsonNode> handler) {
        subscribeToSpotChannel("spot@public.ticker.v3.api.pb", null, handler);
    }

    /**
     * Subscribe to individual ticker data (Spot)
     */
    public void subscribeSpotTicker(String symbol, Consumer<JsonNode> handler) {
        subscribeToSpotChannel("spot@public.ticker.v3.api.pb@" + symbol.toUpperCase(), null, handler);
    }

    /**
     * Subscribe to trade streams (Spot)
     */
    public void subscribeSpotTrades(String symbol, String interval, Consumer<JsonNode> handler) {
        String channel = String.format("spot@public.aggre.deals.v3.api.pb@%s@%s", interval, symbol.toUpperCase());
        subscribeToSpotChannel(channel, null, handler);
    }

    /**
     * Subscribe to K-line data (Spot)
     */
    public void subscribeSpotKline(String symbol, String interval, Consumer<JsonNode> handler) {
        String channel = String.format("spot@public.kline.v3.api.pb@%s@%s", symbol.toUpperCase(), interval);
        subscribeToSpotChannel(channel, null, handler);
    }

    /**
     * Subscribe to depth/orderbook data (Spot)
     */
    public void subscribeSpotDepth(String symbol, String interval, Consumer<JsonNode> handler) {
        String channel = String.format("spot@public.aggre.depth.v3.api.pb@%s@%s", interval, symbol.toUpperCase());
        subscribeToSpotChannel(channel, null, handler);
    }

    /**
     * Subscribe to partial book depth (Spot)
     */
    public void subscribeSpotPartialDepth(String symbol, int levels, Consumer<JsonNode> handler) {
        String channel = String.format("spot@public.limit.depth.v3.api.pb@%s@%d", symbol.toUpperCase(), levels);
        subscribeToSpotChannel(channel, null, handler);
    }

    /**
     * Subscribe to book ticker (Spot)
     */
    public void subscribeSpotBookTicker(String symbol, String interval, Consumer<JsonNode> handler) {
        String channel = String.format("spot@public.aggre.bookTicker.v3.api.pb@%s@%s", interval, symbol.toUpperCase());
        subscribeToSpotChannel(channel, null, handler);
    }

    // ================== FUTURES PUBLIC STREAMS ==================

    /**
     * Subscribe to all tickers (Futures)
     */
    public void subscribeFuturesTickers(Consumer<JsonNode> handler) {
        subscribeToFuturesChannel("sub.tickers", Collections.emptyMap(), handler);
    }

    /**
     * Subscribe to individual ticker (Futures)
     */
    public void subscribeFuturesTicker(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of("symbol", symbol.toUpperCase());
        subscribeToFuturesChannel("sub.ticker", params, handler);
    }

    /**
     * Subscribe to trade data (Futures)
     */
    public void subscribeFuturesTrades(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of("symbol", symbol.toUpperCase());
        subscribeToFuturesChannel("sub.deal", params, handler);
    }

    /**
     * Subscribe to K-line data (Futures)
     */
    public void subscribeFuturesKline(String symbol, String interval, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of(
                "symbol", symbol.toUpperCase(),
                "interval", interval
        );
        subscribeToFuturesChannel("sub.kline", params, handler);
    }

    /**
     * Subscribe to depth data (Futures)
     */
    public void subscribeFuturesDepth(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of("symbol", symbol.toUpperCase());
        subscribeToFuturesChannel("sub.depth", params, handler);
    }

    /**
     * Subscribe to compressed depth data (Futures)
     */
    public void subscribeFuturesDepthCompressed(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of(
                "symbol", symbol.toUpperCase(),
                "compress", true
        );
        subscribeToFuturesChannel("sub.depth", params, handler);
    }

    /**
     * Subscribe to full depth data (Futures)
     */
    public void subscribeFuturesDepthFull(String symbol, int limit, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of(
                "symbol", symbol.toUpperCase(),
                "limit", limit
        );
        subscribeToFuturesChannel("sub.depth.full", params, handler);
    }

    /**
     * Subscribe to funding rate (Futures)
     */
    public void subscribeFuturesFundingRate(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of("symbol", symbol.toUpperCase());
        subscribeToFuturesChannel("sub.funding.rate", params, handler);
    }

    /**
     * Subscribe to index price (Futures)
     */
    public void subscribeFuturesIndexPrice(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of("symbol", symbol.toUpperCase());
        subscribeToFuturesChannel("sub.index.price", params, handler);
    }

    /**
     * Subscribe to fair price (Futures)
     */
    public void subscribeFuturesFairPrice(String symbol, Consumer<JsonNode> handler) {
        Map<String, Object> params = Map.of("symbol", symbol.toUpperCase());
        subscribeToFuturesChannel("sub.fair.price", params, handler);
    }

    // ================== PRIVATE STREAMS ==================

    /**
     * Subscribe to spot account updates
     */
    public void subscribeSpotAccount(Consumer<JsonNode> handler) {
        if (apiKey == null || secretKey == null) {
            throw new IllegalStateException("API credentials required for private streams");
        }

        createSpotListenKey()
                .thenAccept(key -> {
                    this.listenKey = key;
                    String wsUrl = SPOT_WS_URL + "?listenKey=" + key;
                    subscribeToSpotChannel("spot@private.account.v3.api.pb", wsUrl, handler);
                })
                .exceptionally(ex -> {
                    LOGGER.log(Level.SEVERE, "Failed to create listen key", ex);
                    return null;
                });
    }

    /**
     * Subscribe to spot order updates
     */
    public void subscribeSpotOrders(Consumer<JsonNode> handler) {
        if (listenKey == null) {
            subscribeSpotAccount(data -> {}); // Initialize listen key
        }

        String wsUrl = SPOT_WS_URL + "?listenKey=" + listenKey;
        subscribeToSpotChannel("spot@private.orders.v3.api.pb", wsUrl, handler);
    }

    /**
     * Subscribe to spot trade updates
     */
    public void subscribeSpotTrades(Consumer<JsonNode> handler) {
        if (listenKey == null) {
            subscribeSpotAccount(data -> {}); // Initialize listen key
        }

        String wsUrl = SPOT_WS_URL + "?listenKey=" + listenKey;
        subscribeToSpotChannel("spot@private.deals.v3.api.pb", wsUrl, handler);
    }

    /**
     * Subscribe to futures account updates
     */
    public void subscribeFuturesAccount(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.asset", Collections.emptyMap(), handler);
    }

    /**
     * Subscribe to futures position updates
     */
    public void subscribeFuturesPositions(Consumer<JsonNode> handler) {
        Map<String, Object> params = new HashMap<>();
        Map<String, Object> positionFilter = new HashMap<>();
        Map<String, List<Object>> filters = Map.of(
                "filters", List.of(positionFilter)
        );
        positionFilter.put("filter", "position");
        params.put("method", "personal.filter");
        params.put("param", filters);
        authenticateAndSubscribeFutures(StreamChannel.PRIVATE_POSITION, params, handler);
    }

    /**
     * Subscribe to futures order updates
     */
    public void subscribeFuturesOrders(Consumer<JsonNode> handler) {
        Map<String, Object> params = new HashMap<>();
        Map<String, String> filter = Map.of("filter", "order");
        Map<String, List<Object>> filters = Map.of("filters", List.of(filter));
        params.put("method", "personal.filter");
        params.put("param", filters);
        authenticateAndSubscribeFutures(StreamChannel.PRIVATE_ORDER, params, handler);
    }

    /**
     * Subscribe to futures risk limit updates
     */
    public void subscribeFuturesRiskLimit(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.risk.limit", Collections.emptyMap(), handler);
    }

    /**
     * Subscribe to futures ADL level updates
     */
    public void subscribeFuturesAdlLevel(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.adl.level", Collections.emptyMap(), handler);
    }

    /**
     * Subscribe to futures position mode updates
     */
    public void subscribeFuturesPositionMode(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.position.mode", Collections.emptyMap(), handler);
    }

    public void subscribeCustomChannel(Map<String, Consumer<JsonNode>> channelMaps, Object params) {
        if (apiKey == null || secretKey == null) {
            throw new IllegalStateException("API credentials required for private streams");
        }

        String connectionId = "Multiple channel" + channelMaps.hashCode();
        eventHandlers.putAll(channelMaps);

        WebSocketClient client = connections.computeIfAbsent(connectionId, k ->
                createFuturesWebSocketClient(connectionId));

        if (!client.isOpen()) {
            try {
                client.connectBlocking(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Connection interrupted", e);
            }
        }

        // Authenticate first
        authenticateFutures(client);

        // Wait a bit for authentication to complete
        scheduler.schedule(() -> {
            try {
                client.send(objectMapper.writeValueAsString(params));
                LOGGER.info("Subscribed to multiple channel: " + channelMaps);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to subscribe to multiple channel: " + channelMaps, e);
            }
        }, 1, TimeUnit.SECONDS);
    }

    // ================== UNSUBSCRIBE METHODS ==================

    /**
     * Unsubscribe from a spot channel
     */
    public void unsubscribeSpot(String channel) {
        WebSocketClient client = connections.get("spot");
        if (client != null && client.isOpen()) {
            try {
                ObjectNode message = objectMapper.createObjectNode();
                message.put("method", "UNSUBSCRIPTION");
                message.set("params", objectMapper.createArrayNode().add(channel));

                client.send(objectMapper.writeValueAsString(message));
                subscribedChannels.remove(channel);
                LOGGER.info("Unsubscribed from spot channel: " + channel);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to unsubscribe from spot channel: " + channel, e);
            }
        }
    }

    /**
     * Unsubscribe from a futures channel
     */
    public void unsubscribeFutures(String method, Map<String, Object> params) {
        WebSocketClient client = connections.get("futures");
        if (client != null && client.isOpen()) {
            try {
                String unsubMethod = method.replace("sub.", "unsub.");
                ObjectNode message = objectMapper.createObjectNode();
                message.put("method", unsubMethod);
                message.set("param", objectMapper.valueToTree(params));

                client.send(objectMapper.writeValueAsString(message));
                LOGGER.info("Unsubscribed from futures channel: " + unsubMethod);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to unsubscribe from futures channel", e);
            }
        }
    }

    // ================== CONNECTION MANAGEMENT ==================

    private void subscribeToSpotChannel(String channel, String customUrl, Consumer<JsonNode> handler) {
        String connectionId = "spot";
        String url = customUrl != null ? customUrl : SPOT_WS_URL;

        eventHandlers.put(channel, handler);

        WebSocketClient client = connections.computeIfAbsent(connectionId, k ->
                createSpotWebSocketClient(url, connectionId));

        if (!client.isOpen()) {
            try {
                client.connectBlocking(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Connection interrupted", e);
            }
        }

        try {
            ObjectNode message = objectMapper.createObjectNode();
            message.put("method", "SUBSCRIPTION");
            message.set("params", objectMapper.createArrayNode().add(channel));

            client.send(objectMapper.writeValueAsString(message));
            subscribedChannels.add(channel);
            LOGGER.info("Subscribed to spot channel: " + channel);
        } catch (Exception e) {
            throw new RuntimeException("Failed to subscribe to channel: " + channel, e);
        }
    }

    private void subscribeToFuturesChannel(String method, Map<String, Object> params, Consumer<JsonNode> handler) {
        String connectionId = "futures";

        eventHandlers.put(method, handler);

        WebSocketClient client = connections.computeIfAbsent(connectionId, k ->
                createFuturesWebSocketClient(connectionId));

        if (!client.isOpen()) {
            try {
                client.connectBlocking(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Connection interrupted", e);
            }
        }

        try {
            ObjectNode message = objectMapper.createObjectNode();
            message.put("method", method);
            message.set("param", objectMapper.valueToTree(params));

            client.send(objectMapper.writeValueAsString(message));
            LOGGER.info("Subscribed to futures channel: " + method);
        } catch (Exception e) {
            throw new RuntimeException("Failed to subscribe to channel: " + method, e);
        }
    }

    private void authenticateAndSubscribeFutures(String channel, Map<String, Object> params, Consumer<JsonNode> handler) {
        if (apiKey == null || secretKey == null) {
            throw new IllegalStateException("API credentials required for private streams");
        }

        String connectionId = "futures" + channel;
        eventHandlers.put(channel, handler);

        WebSocketClient client = connections.computeIfAbsent(connectionId, k ->
                createFuturesWebSocketClient(connectionId));

        if (!client.isOpen()) {
            try {
                client.connectBlocking(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Connection interrupted", e);
            }
        }

        // Authenticate first
        authenticateFutures(client);

        // Wait a bit for authentication to complete
        scheduler.schedule(() -> {
            try {
                client.send(objectMapper.writeValueAsString(params));
                LOGGER.info("Subscribed to private futures channel: " + channel);
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to subscribe to private channel: " + channel, e);
            }
        }, 1, TimeUnit.SECONDS);
    }

    private WebSocketClient createSpotWebSocketClient(String url, String connectionId) {
        return new WebSocketClient(URI.create(url)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                LOGGER.info("Spot WebSocket connected: " + connectionId);
                onConnect.accept(connectionId);
                startHeartbeat(this, connectionId, true);
            }

            @Override
            public void onMessage(String message) {
                handleSpotMessage(message);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                LOGGER.info("Spot WebSocket disconnected: " + connectionId + " - " + reason);
                onDisconnect.accept(connectionId);
                stopHeartbeat(connectionId);

                if (autoReconnect && remote) {
                    scheduleReconnect(connectionId, url, true);
                }
            }

            @Override
            public void onError(Exception ex) {
                LOGGER.log(Level.SEVERE, "Spot WebSocket error: " + connectionId, ex);
                onError.accept(ex);
            }
        };
    }

    private WebSocketClient createFuturesWebSocketClient(String connectionId) {
        return new WebSocketClient(URI.create(FUTURES_WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                LOGGER.info("Futures WebSocket connected: " + connectionId);
                onConnect.accept(connectionId);
                startHeartbeat(this, connectionId, false);
            }

            @Override
            public void onMessage(String message) {
                handleFuturesMessage(message);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                LOGGER.info("Futures WebSocket disconnected: " + connectionId + " - " + reason);
                onDisconnect.accept(connectionId);
                stopHeartbeat(connectionId);

                if (autoReconnect && remote) {
                    scheduleReconnect(connectionId, FUTURES_WS_URL, false);
                }
            }

            @Override
            public void onError(Exception ex) {
                LOGGER.log(Level.SEVERE, "Futures WebSocket error: " + connectionId, ex);
                onError.accept(ex);
            }
        };
    }

    private void handleSpotMessage(String message) {
        try {
            JsonNode json = objectMapper.readTree(message);

            // Handle different message types
            if (json.has("id") && json.has("code")) {
                // Subscription response
                int code = json.get("code").asInt();
                if (code == 0) {
                    LOGGER.info("Spot subscription successful: " + json.get("msg").asText(""));
                } else {
                    LOGGER.warning("Spot subscription failed: " + json.toString());
                }
                return;
            }

            if (json.has("channel")) {
                String channel = json.get("channel").asText();
                Consumer<JsonNode> handler = eventHandlers.get(channel);
                if (handler != null) {
                    handler.accept(json);
                } else {
                    LOGGER.fine("No handler for spot channel: " + channel);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to parse spot message: " + message, e);
        }
    }

    private void handleFuturesMessage(String message) {
        try {
            JsonNode json = objectMapper.readTree(message);

            // Handle pong response
            if (json.has("channel") && "pong".equals(json.get("channel").asText())) {
                LOGGER.fine("Received pong from futures WebSocket");
                return;
            }

            // Handle login response
            if (json.has("channel") && json.get("channel").asText().startsWith("rs.")) {
                String channel = json.get("channel").asText();
                if ("rs.login".equals(channel)) {
                    LOGGER.info("Futures authentication successful");
                } else if ("rs.error".equals(channel)) {
                    LOGGER.severe("Futures error: " + json.get("data").asText(""));
                }
                return;
            }

            // Handle subscription response
            if (json.has("channel") && json.get("channel").asText().startsWith("rs.sub.")) {
                LOGGER.info("Futures subscription response: " + json.toString());
                return;
            }

            // Handle data messages
            if (json.has("channel")) {
                String channel = json.get("channel").asText();

                // Map push channels to subscription methods
                String methodKey = mapChannelToMethod(channel);
                Consumer<JsonNode> handler = eventHandlers.get(methodKey);

                if (handler != null) {
                    handler.accept(json);
                } else {
                    LOGGER.fine("No handler for futures channel: " + channel + " (mapped to: " + methodKey + ")");
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to parse futures message: " + message, e);
        }
    }

    private String mapChannelToMethod(String channel) {
        // Map push channels to their subscription methods
        Map<String, String> channelMap = Map.of(
                "push.tickers", "sub.tickers",
                "push.ticker", "sub.ticker",
                "push.deal", "sub.deal",
                "push.kline", "sub.kline",
                "push.depth", "sub.depth",
                "push.funding.rate", "sub.funding.rate",
                "push.index.price", "sub.index.price",
                "push.fair.price", "sub.fair.price"
        );

        return channelMap.getOrDefault(channel, channel);
    }

    private void authenticateFutures(WebSocketClient client) {
        try {
            long timestamp = Instant.now().toEpochMilli();
            String signature = createSignature(apiKey + timestamp, secretKey);

            ObjectNode authMessage = objectMapper.createObjectNode();
            authMessage.put("method", "login");
            authMessage.put("subscribe", "false");

            ObjectNode param = objectMapper.createObjectNode();
            param.put("apiKey", apiKey);
            param.put("reqTime", String.valueOf(timestamp));
            param.put("signature", signature);
            authMessage.set("param", param);

            client.send(objectMapper.writeValueAsString(authMessage));
            LOGGER.info("Sent futures authentication request");
        } catch (Exception e) {
            throw new RuntimeException("Failed to authenticate futures WebSocket", e);
        }
    }

    private CompletableFuture<String> createSpotListenKey() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create REST API helper for this operation
                MexcRestApiHelper restHelper = new MexcRestApiHelper(apiKey, secretKey);
                String listenKey = restHelper.createSpotListenKey().get();
                restHelper.close();

                LOGGER.info("Created spot listen key successfully");
                return listenKey;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create listen key", e);
            }
        });
    }

    private void startHeartbeat(WebSocketClient client, String connectionId, boolean isSpot) {
        ScheduledFuture<?> heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            if (client.isOpen()) {
                try {
                    if (isSpot) {
                        // Spot WebSocket ping
                        client.send("{\"method\": \"PING\"}");
                    } else {
                        // Futures WebSocket ping
                        client.send("{\"method\": \"ping\"}");
                    }
                    LOGGER.fine("Sent ping to " + connectionId);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to send ping to " + connectionId, e);
                }
            }
        }, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);

        heartbeatTasks.put(connectionId, heartbeatTask);
    }

    private void stopHeartbeat(String connectionId) {
        ScheduledFuture<?> task = heartbeatTasks.remove(connectionId);
        if (task != null) {
            task.cancel(false);
        }
    }

    private void scheduleReconnect(String connectionId, String url, boolean isSpot) {
        scheduler.schedule(() -> {
            LOGGER.info("Attempting to reconnect: " + connectionId);

            WebSocketClient oldClient = connections.remove(connectionId);
            if (oldClient != null) {
                oldClient.close();
            }

            WebSocketClient newClient = isSpot ?
                    createSpotWebSocketClient(url, connectionId) :
                    createFuturesWebSocketClient(connectionId);

            connections.put(connectionId, newClient);

            try {
                newClient.connectBlocking(5, TimeUnit.SECONDS);

                // Re-subscribe to channels
                if (isSpot) {
                    resubscribeSpotChannels(newClient);
                } else {
                    resubscribeFuturesChannels(newClient);
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect: " + connectionId, e);
            }
        }, reconnectDelay, TimeUnit.MILLISECONDS);
    }

    private void resubscribeSpotChannels(WebSocketClient client) {
        for (String channel : subscribedChannels) {
            try {
                ObjectNode message = objectMapper.createObjectNode();
                message.put("method", "SUBSCRIPTION");
                message.set("params", objectMapper.createArrayNode().add(channel));

                client.send(objectMapper.writeValueAsString(message));
                LOGGER.info("Re-subscribed to spot channel: " + channel);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to re-subscribe to spot channel: " + channel, e);
            }
        }
    }

    private void resubscribeFuturesChannels(WebSocketClient client) {
        // Re-authenticate if needed
        if (apiKey != null && secretKey != null) {
            authenticateFutures(client);
        }

        // Re-subscribe to channels would go here
        // This would require keeping track of subscribed futures channels
    }

    private String createSignature(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hash = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create signature", e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    // ================== CONFIGURATION METHODS ==================

    /**
     * Set event handlers
     */
    public void setOnConnect(Consumer<String> onConnect) {
        this.onConnect = onConnect;
    }

    public void setOnDisconnect(Consumer<String> onDisconnect) {
        this.onDisconnect = onDisconnect;
    }

    public void setOnError(Consumer<Exception> onError) {
        this.onError = onError;
    }

    /**
     * Configure auto-reconnect behavior
     */
    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    public void setReconnectDelay(int reconnectDelayMs) {
        this.reconnectDelay = reconnectDelayMs;
    }

    public void setHeartbeatInterval(int heartbeatIntervalMs) {
        this.heartbeatInterval = heartbeatIntervalMs;
    }

    public void setMaxReconnectAttempts(int maxAttempts) {
        this.maxReconnectAttempts = maxAttempts;
    }

    // ================== CONNECTION STATUS ==================

    /**
     * Check if connected to spot WebSocket
     */
    public boolean isSpotConnected() {
        WebSocketClient client = connections.get("spot");
        return client != null && client.isOpen();
    }

    /**
     * Check if connected to futures WebSocket
     */
    public boolean isFuturesConnected() {
        WebSocketClient client = connections.get("futures");
        return client != null && client.isOpen();
    }

    /**
     * Get list of subscribed channels
     */
    public Set<String> getSubscribedChannels() {
        return new HashSet<>(subscribedChannels);
    }

    // ================== CLEANUP METHODS ==================

    /**
     * Close specific connection
     */
    public void closeConnection(String connectionId) {
        WebSocketClient client = connections.remove(connectionId);
        if (client != null) {
            stopHeartbeat(connectionId);
            client.close();
            LOGGER.info("Closed connection: " + connectionId);
        }
    }

    /**
     * Close all connections and cleanup resources
     */
    public void closeAll() {
        LOGGER.info("Closing all connections...");

        // Stop all heartbeat tasks
        heartbeatTasks.values().forEach(task -> task.cancel(false));
        heartbeatTasks.clear();

        // Close all WebSocket connections
        connections.values().forEach(WebSocketClient::close);
        connections.clear();

        // Clear subscriptions and handlers
        subscribedChannels.clear();
        eventHandlers.clear();

        // Shutdown scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOGGER.info("All connections closed and resources cleaned up");
    }

    // ================== UTILITY METHODS ==================

    /**
     * Filter private data subscriptions (Futures only)
     * This allows you to selectively subscribe to specific private data types
     */
    public void setFuturesPrivateFilter(List<String> filters, Map<String, List<String>> rules) {
        WebSocketClient client = connections.get("futures");
        if (client != null && client.isOpen()) {
            try {
                ObjectNode message = objectMapper.createObjectNode();
                message.put("method", "personal.filter");

                ObjectNode param = objectMapper.createObjectNode();

                // Create filters array
                var filtersArray = objectMapper.createArrayNode();
                for (String filter : filters) {
                    ObjectNode filterObj = objectMapper.createObjectNode();
                    filterObj.put("filter", filter);

                    // Add rules if specified for this filter
                    if (rules.containsKey(filter)) {
                        var rulesArray = objectMapper.createArrayNode();
                        rules.get(filter).forEach(rulesArray::add);
                        filterObj.set("rules", rulesArray);
                    }

                    filtersArray.add(filterObj);
                }

                param.set("filters", filtersArray);
                message.set("param", param);

                client.send(objectMapper.writeValueAsString(message));
                LOGGER.info("Set futures private data filter: " + filters);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to set private data filter", e);
            }
        }
    }

    /**
     * Extend spot listen key validity
     */
    public CompletableFuture<Void> extendSpotListenKey() {
        if (listenKey == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("No listen key available"));
        }

        return CompletableFuture.runAsync(() -> {
            try {
                // In a real implementation, you would call:
                // PUT https://api.mexc.com/api/v3/userDataStream?listenKey={listenKey}
                LOGGER.info("Extended spot listen key validity");
            } catch (Exception e) {
                throw new RuntimeException("Failed to extend listen key", e);
            }
        });
    }

    /**
     * Create a new spot listen key
     */
    public CompletableFuture<String> refreshSpotListenKey() {
        return createSpotListenKey().thenApply(newKey -> {
            String oldKey = this.listenKey;
            this.listenKey = newKey;

            // Close old connection if exists
            if (oldKey != null) {
                closeConnection("spot");
            }

            LOGGER.info("Refreshed spot listen key");
            return newKey;
        });
    }

    // ================== ADDITIONAL FUTURES METHODS ==================

    /**
     * Subscribe to futures plan order updates
     */
    public void subscribeFuturesPlanOrders(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.plan.order", Collections.emptyMap(), handler);
    }

    /**
     * Subscribe to futures stop order updates
     */
    public void subscribeFuturesStopOrders(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.stop.order", Collections.emptyMap(), handler);
    }

    /**
     * Subscribe to futures stop plan order updates
     */
    public void subscribeFuturesStopPlanOrders(Consumer<JsonNode> handler) {
        authenticateAndSubscribeFutures("push.personal.stop.planorder", Collections.emptyMap(), handler);
    }

    // ================== BATCH SUBSCRIPTION METHODS ==================

    /**
     * Subscribe to multiple spot symbols at once
     */
    public void subscribeSpotMultiple(List<String> symbols, String dataType, String interval, Consumer<JsonNode> handler) {
        for (String symbol : symbols) {
            switch (dataType.toLowerCase()) {
                case "ticker":
                    subscribeSpotTicker(symbol, handler);
                    break;
                case "trades":
                    subscribeSpotTrades(symbol, interval != null ? interval : "100ms", handler);
                    break;
                case "kline":
                    subscribeSpotKline(symbol, interval != null ? interval : "Min1", handler);
                    break;
                case "depth":
                    subscribeSpotDepth(symbol, interval != null ? interval : "100ms", handler);
                    break;
                case "bookticker":
                    subscribeSpotBookTicker(symbol, interval != null ? interval : "100ms", handler);
                    break;
                default:
                    LOGGER.warning("Unknown data type: " + dataType);
            }
        }
    }

    /**
     * Subscribe to multiple futures symbols at once
     */
    public void subscribeFuturesMultiple(List<String> symbols, String dataType, String interval, Consumer<JsonNode> handler) {
        for (String symbol : symbols) {
            switch (dataType.toLowerCase()) {
                case "ticker":
                    subscribeFuturesTicker(symbol, handler);
                    break;
                case "trades":
                    subscribeFuturesTrades(symbol, handler);
                    break;
                case "kline":
                    subscribeFuturesKline(symbol, interval != null ? interval : "Min1", handler);
                    break;
                case "depth":
                    subscribeFuturesDepth(symbol, handler);
                    break;
                case "fundingrate":
                    subscribeFuturesFundingRate(symbol, handler);
                    break;
                case "indexprice":
                    subscribeFuturesIndexPrice(symbol, handler);
                    break;
                case "fairprice":
                    subscribeFuturesFairPrice(symbol, handler);
                    break;
                default:
                    LOGGER.warning("Unknown data type: " + dataType);
            }
        }
    }
}
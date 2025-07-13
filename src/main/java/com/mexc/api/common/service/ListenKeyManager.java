package com.mexc.api.common.service;

import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ListenKeyManager {
    private static final Logger LOGGER = Logger.getLogger(ListenKeyManager.class.getName());

    private final MexcRestApiHelper restApi;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private String currentListenKey;
    private ScheduledFuture<?> extensionTask;

    public ListenKeyManager(MexcRestApiHelper restApi) {
        this.restApi = restApi;
    }

    /**
     * Initialize a new listen key and start automatic extension
     */
    public CompletableFuture<String> initialize() {
        return restApi.createSpotListenKey()
                .thenApply(listenKey -> {
                    this.currentListenKey = listenKey;
                    startAutoExtension();
                    return listenKey;
                });
    }

    /**
     * Get the current listen key
     */
    public String getCurrentListenKey() {
        return currentListenKey;
    }

    /**
     * Manually refresh the listen key
     */
    public CompletableFuture<String> refresh() {
        stopAutoExtension();

        // Delete old key if exists
        CompletableFuture<Void> deleteOld = currentListenKey != null ?
                restApi.deleteSpotListenKey(currentListenKey) :
                CompletableFuture.completedFuture(null);

        return deleteOld.thenCompose(v -> restApi.createSpotListenKey())
                .thenApply(newKey -> {
                    this.currentListenKey = newKey;
                    startAutoExtension();
                    return newKey;
                });
    }

    /**
     * Start automatic listen key extension every 30 minutes
     */
    private void startAutoExtension() {
        extensionTask = scheduler.scheduleAtFixedRate(() -> {
            if (currentListenKey != null) {
                restApi.extendSpotListenKey(currentListenKey)
                        .thenRun(() -> LOGGER.info("Successfully extended listen key"))
                        .exceptionally(ex -> {
                            LOGGER.log(Level.WARNING, "Failed to extend listen key, will refresh", ex);
                            refresh();
                            return null;
                        });
            }
        }, 30, 30, TimeUnit.MINUTES);
    }

    /**
     * Stop automatic extension
     */
    private void stopAutoExtension() {
        if (extensionTask != null) {
            extensionTask.cancel(false);
            extensionTask = null;
        }
    }

    /**
     * Cleanup resources
     */
    public void shutdown() {
        stopAutoExtension();

        if (currentListenKey != null) {
            restApi.deleteSpotListenKey(currentListenKey)
                    .thenRun(() -> LOGGER.info("Deleted listen key on shutdown"))
                    .exceptionally(ex -> {
                        LOGGER.log(Level.WARNING, "Failed to delete listen key on shutdown", ex);
                        return null;
                    });
        }

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

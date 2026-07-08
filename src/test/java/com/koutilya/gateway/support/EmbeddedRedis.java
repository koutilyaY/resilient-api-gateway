package com.koutilya.gateway.support;

import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Small lifecycle helper around {@link RedisServer} (codemonstur embedded-redis, which ships an
 * arm64 binary so it runs on Apple Silicon). Picks a free port so parallel/repeated runs don't
 * collide, and exposes it for {@code @DynamicPropertySource} wiring.
 */
public final class EmbeddedRedis {

    private final int port;
    private final RedisServer server;

    private EmbeddedRedis(int port, RedisServer server) {
        this.port = port;
        this.server = server;
    }

    public static EmbeddedRedis startOnRandomPort() {
        try {
            int port = freePort();
            RedisServer server = new RedisServer(port);
            server.start();
            return new EmbeddedRedis(port, server);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start embedded Redis", e);
        }
    }

    public int port() {
        return port;
    }

    public void stop() {
        try {
            server.stop();
        } catch (IOException e) {
            // best-effort teardown
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

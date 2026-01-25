package com.redis.server;

import com.redis.config.RedisConfig;
import com.redis.storage.RedisDatabase;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Main Netty Redis server.
 * Sets up Boss Group (accepts connections) and Worker Group (handles I/O).
 * Uses single-threaded worker model for consistency with Redis's single-threaded design.
 */
public class NettyRedisServer {

    private final RedisConfig config;

    public NettyRedisServer(RedisConfig config) {
        this.config = config;
    }

    /**
     * Starts the Netty-based Redis server, binds it to the configured port, and blocks until the server channel closes.
     *
     * The method initializes boss and worker event loop groups, configures the server bootstrap to accept
     * connections and install a Redis command handler for each channel, and then binds to the configured port.
     * When the server stops (or an exception occurs), event loop groups and the RedisDatabase singleton are shut down.
     *
     * @throws Exception if the server fails to bind, is interrupted while waiting, or another error occurs during startup or runtime
     */
    public void run() throws Exception {
        /*
         * Boss Group: handles incoming connection requests.
         * Typically, uses 1 thread for simple servers.
         */
        EventLoopGroup bossGroup = new NioEventLoopGroup(config.getBossThreads());

        /*
         * Worker Group: handles I/O for accepted connections.
         * A single-threaded model enforces Redis-like sequential command processing.
         * CRITICAL: Set to 1 to ensure single-threaded semantics without explicit locking.
         */
        EventLoopGroup workerGroup = new NioEventLoopGroup(config.getWorkerThreads());

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new RedisCommandHandler());
                        }
                    });

            System.out.println("[Redis] Server starting on port: " + config.getPort());

            // Bind and start to accept incoming connections
            ChannelFuture f = b.bind(config.getPort()).sync();

            // Wait until the server socket is closed
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            RedisDatabase.getInstance().shutdown();
            System.out.println("[Redis] Server stopped");
        }
    }

    public static void main(String[] args) throws Exception {
        RedisConfig config = RedisConfig.getInstance();
        new NettyRedisServer(config).run();
    }
}
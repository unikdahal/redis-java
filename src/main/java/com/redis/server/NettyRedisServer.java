package com.redis.server;

import com.redis.config.RedisConfig;
import com.redis.replication.MasterConnection;
import com.redis.replication.ReplicationManager;
import com.redis.replication.ServerRole;
import com.redis.storage.RedisDatabase;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Main Netty Redis server with master-slave replication support.
 * <p>
 * <b>Features:</b>
 * <ul>
 *   <li>Boss Group - accepts connections</li>
 *   <li>Worker Group - handles I/O</li>
 *   <li>Single-threaded worker model for Redis-like consistency</li>
 *   <li>Master-slave replication support</li>
 * </ul>
 * <p>
 * <b>Command-Line Arguments:</b>
 * <ul>
 *   <li>--port &lt;port&gt; - Server listening port</li>
 *   <li>--replicaof &lt;host&gt; &lt;port&gt; - Configure as replica</li>
 * </ul>
 */
public class NettyRedisServer {

    private final RedisConfig config;

    public NettyRedisServer(RedisConfig config) {
        this.config = config;
    }

    public void run() throws Exception {
        // Initialize replication
        initReplication();

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

            ReplicationManager replMgr = ReplicationManager.getInstance();
            String roleStr = replMgr.isMaster() ? "master" : "slave";

            System.out.println("[Redis] Server starting on port: " + config.getPort());
            System.out.println("[Redis] Role: " + roleStr);
            System.out.println("[Redis] Replication ID: " + replMgr.getMasterReplId());

            if (config.isReplica()) {
                System.out.println("[Redis] Replicating from: " + config.getReplicaOfHost()
                    + ":" + config.getReplicaOfPort());
            }

            // Bind and start to accept incoming connections
            ChannelFuture f = b.bind(config.getPort()).sync();

            // If we're a replica, connect to master after server is bound
            if (config.isReplica()) {
                connectToMaster();
            }

            // Wait until the server socket is closed
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();

            // Cleanup replication
            ReplicationManager replMgr = ReplicationManager.getInstance();
            if (replMgr.getMasterConnection() != null) {
                replMgr.getMasterConnection().disconnect();
            }

            RedisDatabase.getInstance().shutdown();
            System.out.println("[Redis] Server stopped");
        }
    }

    /**
     * Initializes replication based on configuration.
     */
    private void initReplication() {
        ReplicationManager replMgr = ReplicationManager.getInstance();

        if (config.isReplica()) {
            replMgr.setRole(ServerRole.SLAVE);
            replMgr.setMasterInfo(config.getReplicaOfHost(), config.getReplicaOfPort());
        } else {
            replMgr.setRole(ServerRole.MASTER);
        }
    }

    /**
     * Connects to the master server and initiates replication handshake.
     */
    private void connectToMaster() {
        ReplicationManager replMgr = ReplicationManager.getInstance();

        MasterConnection masterConn = new MasterConnection(
            config.getReplicaOfHost(),
            config.getReplicaOfPort(),
            config.getPort()
        );

        replMgr.setMasterConnection(masterConn);

        // Connect asynchronously
        masterConn.connect().whenComplete((result, error) -> {
            if (error != null) {
                System.err.println("[Redis] Failed to connect to master: " + error.getMessage());
            }
        });
    }

    public static void main(String[] args) throws Exception {
        RedisConfig config = RedisConfig.getInstance();

        // Parse command-line arguments
        config.parseArgs(args);

        System.out.println("[Redis] Configuration: " + config);

        new NettyRedisServer(config).run();
    }
}

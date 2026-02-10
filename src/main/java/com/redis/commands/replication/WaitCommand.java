package com.redis.commands.replication;

import com.redis.commands.ICommand;
import com.redis.replication.ReplicationManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * WAIT command implementation.
 * <p>
 * <b>Syntax:</b> WAIT &lt;numreplicas&gt; &lt;timeout&gt;
 * <p>
 * Blocks the client until:
 * <ul>
 *   <li>The specified number of replicas have acknowledged the current offset</li>
 *   <li>The timeout expires</li>
 * </ul>
 * <p>
 * <b>Returns:</b> The number of replicas that acknowledged within the timeout.
 * <p>
 * <b>Implementation Notes:</b>
 * <ul>
 *   <li>If timeout is 0, waits indefinitely until numreplicas acknowledge</li>
 *   <li>If numreplicas is 0, returns immediately with connected replica count</li>
 *   <li>Sends REPLCONF GETACK to request acknowledgment from replicas</li>
 *   <li>Polls replica acknowledgment status until condition is met or timeout</li>
 *   <li>Offloads blocking work to a dedicated thread pool to avoid blocking Netty's event loop</li>
 * </ul>
 */
public class WaitCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'WAIT' command\r\n";
    private static final String ERR_INVALID_NUM = "-ERR numreplicas is not a non-negative integer\r\n";
    private static final String ERR_INVALID_TIMEOUT = "-ERR timeout is not a non-negative integer\r\n";

    /** Dedicated thread pool for blocking WAIT operations to avoid blocking Netty's event loop */
    private static final ExecutorService WAIT_EXECUTOR = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "wait-cmd-worker");
        t.setDaemon(true);
        return t;
    });

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        int numReplicas;
        long timeoutMs;

        try {
            numReplicas = Integer.parseInt(args.get(0));
            if (numReplicas < 0) {
                return ERR_INVALID_NUM;
            }
        } catch (NumberFormatException e) {
            return ERR_INVALID_NUM;
        }

        try {
            timeoutMs = Long.parseLong(args.get(1));
            if (timeoutMs < 0) {
                return ERR_INVALID_TIMEOUT;
            }
        } catch (NumberFormatException e) {
            return ERR_INVALID_TIMEOUT;
        }

        ReplicationManager replMgr = ReplicationManager.getInstance();

        // If this server is a slave, return 0
        if (replMgr.isSlave()) {
            return ":0\r\n";
        }

        // Special case: if no replicas needed, return immediately
        if (numReplicas == 0) {
            return ":" + replMgr.getConnectedReplicaCount() + "\r\n";
        }

        // Offload blocking operation to dedicated thread pool
        // to avoid blocking Netty's event loop
        final long effectiveTimeoutMs = timeoutMs;
        CompletableFuture.supplyAsync(() ->
            replMgr.waitForReplicas(numReplicas, effectiveTimeoutMs), WAIT_EXECUTOR)
            .whenComplete((acknowledged, ex) -> {
                // Marshal the response back to Netty's event loop
                ctx.channel().eventLoop().execute(() -> {
                    if (ex != null) {
                        ctx.writeAndFlush(io.netty.buffer.Unpooled.copiedBuffer(
                            "-ERR internal error\r\n", java.nio.charset.StandardCharsets.UTF_8));
                    } else {
                        ctx.writeAndFlush(io.netty.buffer.Unpooled.copiedBuffer(
                            ":" + acknowledged + "\r\n", java.nio.charset.StandardCharsets.UTF_8));
                    }
                });
            });

        // Return null to indicate async response (handler should not write response)
        return null;
    }

    @Override
    public String name() {
        return "WAIT";
    }
}

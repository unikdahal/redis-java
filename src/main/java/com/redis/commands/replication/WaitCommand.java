package com.redis.commands.replication;

import com.redis.commands.ICommand;
import com.redis.replication.ReplicationManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

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
 *   <li>If timeout is 0 and no writes have occurred, returns immediately with connected replica count</li>
 *   <li>Sends REPLCONF GETACK to request acknowledgment from replicas</li>
 *   <li>Polls replica acknowledgment status until condition is met or timeout</li>
 * </ul>
 */
public class WaitCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'WAIT' command\r\n";
    private static final String ERR_INVALID_NUM = "-ERR numreplicas is not a non-negative integer\r\n";
    private static final String ERR_INVALID_TIMEOUT = "-ERR timeout is not a non-negative integer\r\n";

    /**
     * Execute the WAIT command: validate arguments and wait for the specified number of replicas
     * to acknowledge within the given timeout.
     *
     * @param args a list where args.get(0) is the required number of replicas to wait for (non-negative integer)
     *             and args.get(1) is the timeout in milliseconds (non-negative long)
     * @return `ERR_WRONG_ARGS` if fewer than two arguments are provided,
     *         `ERR_INVALID_NUM` if the replicas argument is not an integer >= 0,
     *         `ERR_INVALID_TIMEOUT` if the timeout argument is not a long >= 0,
     *         otherwise a Redis integer reply of the acknowledged replica count formatted as
     *         ":" + acknowledged + "\r\n"
     */
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

        // Wait for replicas to acknowledge
        int acknowledged = replMgr.waitForReplicas(numReplicas, timeoutMs);

        return ":" + acknowledged + "\r\n";
    }

    /**
     * Get the command name handled by this ICommand implementation.
     *
     * @return the command name "WAIT"
     */
    @Override
    public String name() {
        return "WAIT";
    }
}
package com.redis.commands.replication;

import com.redis.commands.ICommand;
import com.redis.replication.ReplicaConnection;
import com.redis.replication.ReplicationManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * REPLCONF command implementation.
 * <p>
 * Used for replication configuration and acknowledgment between master and replica.
 * <p>
 * <b>Subcommands:</b>
 * <ul>
 *   <li>REPLCONF listening-port &lt;port&gt; - Replica reports its listening port</li>
 *   <li>REPLCONF capa &lt;capability&gt; - Replica announces capability (e.g., psync2)</li>
 *   <li>REPLCONF ACK &lt;offset&gt; - Replica acknowledges processed bytes</li>
 *   <li>REPLCONF GETACK * - Master requests ACK from replica</li>
 * </ul>
 */
public class ReplconfCommand implements ICommand {

    private static final String RESP_OK = "+OK\r\n";
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'REPLCONF' command\r\n";
    private static final String ERR_UNKNOWN_SUBCOMMAND = "-ERR Unknown REPLCONF subcommand\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.isEmpty()) {
            return ERR_WRONG_ARGS;
        }

        String subcommand = args.get(0).toUpperCase();
        ReplicationManager replMgr = ReplicationManager.getInstance();

        switch (subcommand) {
            case "LISTENING-PORT":
                return handleListeningPort(args, ctx, replMgr);

            case "CAPA":
                return handleCapa(args, ctx, replMgr);

            case "ACK":
                return handleAck(args, ctx, replMgr);

            case "GETACK":
                return handleGetAck(args, ctx, replMgr);

            default:
                return ERR_UNKNOWN_SUBCOMMAND;
        }
    }

    /**
     * Handles REPLCONF listening-port from replica.
     * Master uses this to know the replica's listening port.
     */
    private String handleListeningPort(List<String> args, ChannelHandlerContext ctx,
                                        ReplicationManager replMgr) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        try {
            int port = Integer.parseInt(args.get(1));

            // Get or create replica connection
            ReplicaConnection replica = replMgr.getReplica(ctx.channel());
            if (replica == null) {
                String host = ctx.channel().remoteAddress().toString();
                replica = replMgr.addReplica(ctx.channel(), host, port);
            }

            replica.setListeningPort(port);
            replica.setState(ReplicaConnection.ReplicaState.HANDSHAKE);

            System.out.println("[Replication] Replica listening on port: " + port);
            return RESP_OK;

        } catch (NumberFormatException e) {
            return "-ERR invalid port number\r\n";
        }
    }

    /**
     * Handles REPLCONF capa from replica.
     * Replica announces its capabilities (e.g., psync2, eof).
     */
    private String handleCapa(List<String> args, ChannelHandlerContext ctx,
                              ReplicationManager replMgr) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        ReplicaConnection replica = replMgr.getReplica(ctx.channel());
        if (replica != null) {
            String capability = args.get(1).toLowerCase();
            replica.addCapability(capability);
            System.out.println("[Replication] Replica capability: " + capability);
        }

        return RESP_OK;
    }

    /**
     * Handles REPLCONF ACK from replica.
     * Replica reports the number of bytes it has processed.
     */
    private String handleAck(List<String> args, ChannelHandlerContext ctx,
                             ReplicationManager replMgr) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        try {
            long offset = Long.parseLong(args.get(1));

            ReplicaConnection replica = replMgr.getReplica(ctx.channel());
            if (replica != null) {
                replica.updateAcknowledgedOffset(offset);
            }

            // ACK doesn't send a response to avoid infinite loops
            return null;

        } catch (NumberFormatException e) {
            return "-ERR invalid offset\r\n";
        }
    }

    /**
     * Handles REPLCONF GETACK from master.
     * This is received by replicas; they should respond with ACK.
     */
    private String handleGetAck(List<String> args, ChannelHandlerContext ctx,
                                ReplicationManager replMgr) {
        // When we're a slave and receive GETACK, respond with our offset
        if (replMgr.isSlave()) {
            long offset = replMgr.getMasterReplOffset();
            return "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" +
                   String.valueOf(offset).length() + "\r\n" + offset + "\r\n";
        }

        // Masters don't respond to GETACK
        return null;
    }

    @Override
    public String name() {
        return "REPLCONF";
    }
}

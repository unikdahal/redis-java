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

    /**
     * Handles the REPLCONF command by dispatching to the appropriate subcommand handler.
     *
     * @param args the command arguments where args[0] is the REPLCONF subcommand
     *             (e.g., "LISTENING-PORT", "CAPA", "ACK", "GETACK") and subsequent
     *             entries are subcommand-specific parameters
     * @param ctx  the channel handler context for the client connection
     * @return a RESP-formatted response string to send to the client (e.g. "+OK\r\n" or an error),
     *         or `null` when no response should be sent for the subcommand
     */
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
     * Process the REPLCONF LISTENING-PORT subcommand from a replica.
     *
     * Parses the replica's listening port from args, creates or updates the corresponding
     * ReplicaConnection with that port, and transitions the replica to the HANDSHAKE state.
     *
     * @param args   command arguments where args.get(1) is the listening port
     * @param ctx    channel handler context for the replica connection
     * @param replMgr replication manager used to lookup or register the replica
     * @return       {@code +OK\r\n} on success;
     *               {@code -ERR wrong number of arguments for 'REPLCONF' command\r\n} if args are insufficient;
     *               {@code -ERR invalid port number\r\n} if the port is not a valid integer
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
     * Process a REPLCONF CAPA subcommand and record the replica's announced capability.
     *
     * @param args    command arguments where args.get(1) is the capability to add
     * @param ctx     channel handler context identifying the replica connection
     * @param replMgr replication manager used to locate and update the ReplicaConnection
     * @return        RESP_OK after recording the capability, or ERR_WRONG_ARGS if the capability argument is missing
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
     * Process the REPLCONF ACK subcommand from a replica.
     *
     * Updates the replica's acknowledged replication offset based on the offset value provided in the command.
     *
     * @param args command arguments where args.get(1) is the acknowledged offset in bytes
     * @param ctx the channel handler context for the replica connection
     * @param replMgr the replication manager controlling replica state
     * @return "-ERR wrong number of arguments for 'REPLCONF' command\r\n" if arguments are missing,
     *         "-ERR invalid offset\r\n" if the offset cannot be parsed as a number,
     *         `null` on successful processing to indicate no response should be sent
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
     * Responds to a REPLCONF GETACK by returning a RESP multi-bulk containing the current acknowledged offset when running as a replica.
     *
     * @return the RESP multi-bulk string: ["REPLCONF","ACK",<offset>] when this node is a slave; `null` when this node is a master (no response).
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

    /**
     * The identifier for this command implementation used to register and look up the command.
     *
     * @return the command name "REPLCONF"
     */
    @Override
    public String name() {
        return "REPLCONF";
    }
}
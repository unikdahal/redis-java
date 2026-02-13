package com.redis.commands.replication;

import com.redis.commands.ICommand;
import com.redis.replication.RdbGenerator;
import com.redis.replication.ReplicaConnection;
import com.redis.replication.ReplicationManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * PSYNC command implementation.
 * <p>
 * <b>Syntax:</b> PSYNC &lt;replicationid&gt; &lt;offset&gt;
 * <p>
 * Used by replicas to synchronize with the master.
 * <p>
 * <b>Responses:</b>
 * <ul>
 *   <li>+FULLRESYNC &lt;replid&gt; &lt;offset&gt; - Full resync needed, followed by RDB</li>
 *   <li>+CONTINUE [replid] - Partial resync, continue from offset</li>
 *   <li>-ERR ... - Error</li>
 * </ul>
 * <p>
 * <b>Optimization:</b> Uses zero-copy ByteBuf for RDB transfer.
 */
public class PsyncCommand implements ICommand {

    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'PSYNC' command\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        if (args.size() < 2) {
            return ERR_WRONG_ARGS;
        }

        String requestedReplId = args.get(0);
        String requestedOffsetStr = args.get(1);

        ReplicationManager replMgr = ReplicationManager.getInstance();

        // Get replica connection
        ReplicaConnection replica = replMgr.getReplica(ctx.channel());
        if (replica == null) {
            // Create one if not exists (shouldn't happen in normal flow)
            String host = ctx.channel().remoteAddress().toString();
            replica = replMgr.addReplica(ctx.channel(), host, 0);
        }

        // Parse requested offset
        long requestedOffset = -1;
        try {
            requestedOffset = Long.parseLong(requestedOffsetStr);
        } catch (NumberFormatException e) {
            // -1 means full resync requested
        }

        // Determine if we can do partial resync
        boolean canPartialResync = canDoPartialResync(requestedReplId, requestedOffset, replMgr);

        if (canPartialResync) {
            // Partial resync - send missed data from backlog
            return performPartialResync(ctx, replica, replMgr, requestedOffset);
        } else {
            // Full resync
            return performFullResync(ctx, replica, replMgr);
        }
    }

    /**
     * Checks if we can do a partial resync based on replication ID and offset.
     */
    private boolean canDoPartialResync(String requestedReplId, long requestedOffset,
                                        ReplicationManager replMgr) {
        // If replica is asking for first sync ("?" and "-1"), always do full resync
        if ("?".equals(requestedReplId) || requestedOffset == -1) {
            return false;
        }

        // Use ReplicationManager's backlog-based partial resync check
        return replMgr.canPartialResync(requestedReplId, requestedOffset);
    }

    /**
     * Performs a partial resync: sends CONTINUE and backlog data.
     */
    private String performPartialResync(ChannelHandlerContext ctx, ReplicaConnection replica,
                                        ReplicationManager replMgr, long requestedOffset) {
        String replId = replMgr.getMasterReplId();

        // Get backlog data from the requested offset
        byte[] backlogData = replMgr.getBacklogData(requestedOffset);

        // Update replica state
        replica.setState(ReplicaConnection.ReplicaState.STREAMING);

        // Send CONTINUE response
        String continueResponse = "+CONTINUE " + replId + "\r\n";
        ctx.write(Unpooled.copiedBuffer(continueResponse, StandardCharsets.UTF_8));

        // Send missed backlog data if any
        if (backlogData != null && backlogData.length > 0) {
            ctx.writeAndFlush(Unpooled.wrappedBuffer(backlogData));
            System.out.println("[Replication] Partial resync: sent " + backlogData.length + " bytes from backlog");
        } else {
            ctx.flush();
        }

        // Track statistics
        replMgr.incrementPartialResyncs();

        System.out.println("[Replication] Partial resync with replica: " + replica);

        return null; // Already wrote response
    }

    /**
     * Performs a full resync: sends FULLRESYNC response followed by RDB file.
     */
    private String performFullResync(ChannelHandlerContext ctx, ReplicaConnection replica,
                                     ReplicationManager replMgr) {
        String replId = replMgr.getMasterReplId();
        long offset = replMgr.getMasterReplOffset();

        // Update replica state
        replica.setState(ReplicaConnection.ReplicaState.SYNC_REQUESTED);

        // Send FULLRESYNC response
        String fullresyncResponse = "+FULLRESYNC " + replId + " " + offset + "\r\n";
        ctx.write(Unpooled.copiedBuffer(fullresyncResponse, StandardCharsets.UTF_8));

        // Send RDB file
        byte[] rdbData = RdbGenerator.getRdbTransferFormat();
        ctx.writeAndFlush(Unpooled.wrappedBuffer(rdbData));

        // Update replica state to streaming
        replica.setState(ReplicaConnection.ReplicaState.STREAMING);

        // Track statistics
        replMgr.incrementFullResyncs();

        System.out.println("[Replication] Full resync initiated with replica: " + replica);
        System.out.println("[Replication] Sent RDB file (" + rdbData.length + " bytes)");

        // Return null since we've already written the response
        return null;
    }

    @Override
    public String name() {
        return "PSYNC";
    }
}

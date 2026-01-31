package com.redis.commands.transaction;

import com.redis.commands.ICommand;
import com.redis.transaction.TransactionContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for DISCARD command.
 *
 * Test Coverage:
 * - Basic DISCARD execution
 * - DISCARD without MULTI (error case)
 * - Command name verification
 * - Transaction state cleanup
 * - Queued commands clearing
 */
@DisplayName("DISCARD Command Unit Tests")
public class DiscardCommandTest {

    private DiscardCommand command;
    private ChannelHandlerContext mockCtx;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        command = new DiscardCommand();
        channel = new EmbeddedChannel();
        mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.channel()).thenReturn(channel);
    }

    @Test
    @DisplayName("DISCARD: Returns OK when in transaction")
    void testDiscardReturnsOkWhenInTransaction() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertEquals("+OK\r\n", result);
    }

    @Test
    @DisplayName("DISCARD: Returns error when not in transaction")
    void testDiscardReturnsErrorWhenNotInTransaction() {
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("DISCARD: Ends transaction")
    void testDiscardEndsTransaction() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        command.execute(Collections.emptyList(), mockCtx);

        assertFalse(ctx.isInTransaction());
    }

    @Test
    @DisplayName("DISCARD: Clears queued commands")
    void testDiscardClearsQueuedCommands() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();
        ctx.queueCommand(mock(ICommand.class), Collections.emptyList());
        ctx.queueCommand(mock(ICommand.class), Collections.emptyList());

        command.execute(Collections.emptyList(), mockCtx);

        assertEquals(0, ctx.queueSize());
    }

    @Test
    @DisplayName("DISCARD: Clears error flag")
    void testDiscardClearsErrorFlag() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();
        ctx.markError();

        command.execute(Collections.emptyList(), mockCtx);

        assertFalse(ctx.hasErrors());
    }

    @Test
    @DisplayName("DISCARD: Command name is DISCARD")
    void testCommandName() {
        assertEquals("DISCARD", command.name());
    }

    @Test
    @DisplayName("DISCARD: Ignores arguments")
    void testDiscardIgnoresArguments() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        String result = command.execute(Collections.singletonList("extra"), mockCtx);

        assertEquals("+OK\r\n", result);
    }

    @Test
    @DisplayName("DISCARD: Error when transaction context doesn't exist")
    void testDiscardErrorWhenNoContext() {
        // Don't create context
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("DISCARD: Error when context exists but not in transaction")
    void testDiscardErrorWhenContextExistsButNotInTransaction() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        // Don't start transaction

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("DISCARD: Can start new transaction after discard")
    void testCanStartNewTransactionAfterDiscard() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        command.execute(Collections.emptyList(), mockCtx);

        // Should be able to start new transaction
        boolean result = ctx.startTransaction();
        assertTrue(result);
        assertTrue(ctx.isInTransaction());
    }

    @Test
    @DisplayName("DISCARD: Multiple discards without MULTI fail")
    void testMultipleDiscardsWithoutMultiFail() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        command.execute(Collections.emptyList(), mockCtx);

        // Second discard should fail
        String result = command.execute(Collections.emptyList(), mockCtx);
        assertTrue(result.contains("ERR"));
    }

    @Test
    @DisplayName("DISCARD: Works with large queue")
    void testDiscardWithLargeQueue() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        // Queue many commands
        for (int i = 0; i < 1000; i++) {
            ctx.queueCommand(mock(ICommand.class), Collections.emptyList());
        }

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertEquals("+OK\r\n", result);
        assertEquals(0, ctx.queueSize());
    }

    @Test
    @DisplayName("DISCARD: Preserves context object")
    void testDiscardPreservesContextObject() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        command.execute(Collections.emptyList(), mockCtx);

        TransactionContext afterDiscard = TransactionContext.get(channel);
        assertSame(ctx, afterDiscard);
    }

    @Test
    @DisplayName("DISCARD: State is clean after discard")
    void testStateIsCleanAfterDiscard() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();
        ctx.queueCommand(mock(ICommand.class), Collections.emptyList());
        ctx.markError();

        command.execute(Collections.emptyList(), mockCtx);

        assertFalse(ctx.isInTransaction());
        assertFalse(ctx.hasErrors());
        assertEquals(0, ctx.queueSize());
    }
}
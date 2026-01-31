package com.redis.commands.transaction;

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
 * Unit tests for MULTI command.
 *
 * Test Coverage:
 * - Basic MULTI execution
 * - MULTI when already in transaction (nested MULTI)
 * - Command name verification
 * - Transaction context initialization
 * - State management
 */
@DisplayName("MULTI Command Unit Tests")
public class MultiCommandTest {

    private MultiCommand command;
    private ChannelHandlerContext mockCtx;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        command = new MultiCommand();
        channel = new EmbeddedChannel();
        mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.channel()).thenReturn(channel);
    }

    @Test
    @DisplayName("MULTI: Returns OK on success")
    void testMultiReturnsOk() {
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertEquals("+OK\r\n", result);
    }

    @Test
    @DisplayName("MULTI: Creates transaction context")
    void testMultiCreatesTransactionContext() {
        command.execute(Collections.emptyList(), mockCtx);

        TransactionContext ctx = TransactionContext.get(channel);
        assertNotNull(ctx);
        assertTrue(ctx.isInTransaction());
    }

    @Test
    @DisplayName("MULTI: Returns error when already in transaction")
    void testMultiWhenAlreadyInTransaction() {
        // Start first transaction
        command.execute(Collections.emptyList(), mockCtx);

        // Try to start second transaction
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("nested"));
    }

    @Test
    @DisplayName("MULTI: Can be called after DISCARD")
    void testMultiAfterDiscard() {
        // Start and discard transaction
        command.execute(Collections.emptyList(), mockCtx);
        TransactionContext ctx = TransactionContext.get(channel);
        ctx.discard();

        // Should be able to start new transaction
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertEquals("+OK\r\n", result);
        assertTrue(ctx.isInTransaction());
    }

    @Test
    @DisplayName("MULTI: Can be called after EXEC")
    void testMultiAfterExec() {
        // Start and end transaction
        command.execute(Collections.emptyList(), mockCtx);
        TransactionContext ctx = TransactionContext.get(channel);
        ctx.endTransaction();

        // Should be able to start new transaction
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertEquals("+OK\r\n", result);
        assertTrue(ctx.isInTransaction());
    }

    @Test
    @DisplayName("MULTI: Command name is MULTI")
    void testCommandName() {
        assertEquals("MULTI", command.name());
    }

    @Test
    @DisplayName("MULTI: Ignores arguments")
    void testMultiIgnoresArguments() {
        String result = command.execute(Collections.singletonList("extra"), mockCtx);

        assertEquals("+OK\r\n", result);
    }

    @Test
    @DisplayName("MULTI: Transaction context reused if exists")
    void testMultiReusesExistingContext() {
        // Create context first
        TransactionContext ctx1 = TransactionContext.getOrCreate(channel);

        // Call MULTI
        command.execute(Collections.emptyList(), mockCtx);

        TransactionContext ctx2 = TransactionContext.get(channel);

        assertSame(ctx1, ctx2);
    }

    @Test
    @DisplayName("MULTI: Multiple channels have separate contexts")
    void testMultiSeparateContextsPerChannel() {
        EmbeddedChannel channel2 = new EmbeddedChannel();
        ChannelHandlerContext mockCtx2 = mock(ChannelHandlerContext.class);
        when(mockCtx2.channel()).thenReturn(channel2);

        command.execute(Collections.emptyList(), mockCtx);
        command.execute(Collections.emptyList(), mockCtx2);

        TransactionContext ctx1 = TransactionContext.get(channel);
        TransactionContext ctx2 = TransactionContext.get(channel2);

        assertNotSame(ctx1, ctx2);
        assertTrue(ctx1.isInTransaction());
        assertTrue(ctx2.isInTransaction());

        channel2.close();
    }

    @Test
    @DisplayName("MULTI: Transaction starts with empty queue")
    void testMultiStartsWithEmptyQueue() {
        command.execute(Collections.emptyList(), mockCtx);

        TransactionContext ctx = TransactionContext.get(channel);
        assertEquals(0, ctx.queueSize());
        assertFalse(ctx.hasErrors());
    }

    @Test
    @DisplayName("MULTI: Nested MULTI preserves first transaction state")
    void testNestedMultiPreservesState() {
        command.execute(Collections.emptyList(), mockCtx);
        TransactionContext ctx = TransactionContext.get(channel);

        // Queue a command
        ctx.queueCommand(mock(com.redis.commands.ICommand.class), Collections.emptyList());

        // Try nested MULTI
        String result = command.execute(Collections.emptyList(), mockCtx);

        // Should still be in transaction with queued command
        assertTrue(ctx.isInTransaction());
        assertEquals(1, ctx.queueSize());
        assertTrue(result.contains("ERR"));
    }
}
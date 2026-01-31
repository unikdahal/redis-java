package com.redis.transaction;

import com.redis.commands.ICommand;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for TransactionContext.
 * Tests the transaction state management logic in isolation.
 *
 * Test Coverage:
 * - Transaction lifecycle (start, queue, discard, end)
 * - Command queueing and retrieval
 * - Error tracking
 * - State transitions
 * - Edge cases
 */
@DisplayName("TransactionContext Unit Tests")
public class TransactionContextTest {

    private EmbeddedChannel channel;
    private TransactionContext context;
    private ICommand mockCommand;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        context = new TransactionContext();
        mockCommand = mock(ICommand.class);
    }

    @Test
    @DisplayName("New context is not in transaction")
    void testNewContextNotInTransaction() {
        assertFalse(context.isInTransaction());
        assertFalse(context.hasErrors());
        assertEquals(0, context.queueSize());
    }

    @Test
    @DisplayName("startTransaction initializes transaction state")
    void testStartTransaction() {
        boolean result = context.startTransaction();

        assertTrue(result);
        assertTrue(context.isInTransaction());
        assertFalse(context.hasErrors());
        assertEquals(0, context.queueSize());
    }

    @Test
    @DisplayName("startTransaction fails when already in transaction")
    void testStartTransactionWhenAlreadyInTransaction() {
        context.startTransaction();

        boolean result = context.startTransaction();

        assertFalse(result);
        assertTrue(context.isInTransaction());
    }

    @Test
    @DisplayName("queueCommand adds command to queue")
    void testQueueCommand() {
        context.startTransaction();
        List<String> args = Arrays.asList("key", "value");

        context.queueCommand(mockCommand, args);

        assertEquals(1, context.queueSize());
        List<TransactionContext.QueuedCommand> queuedCommands = context.getQueuedCommands();
        assertEquals(1, queuedCommands.size());
        assertEquals(mockCommand, queuedCommands.get(0).command());
        assertEquals(args, queuedCommands.get(0).args());
    }

    @Test
    @DisplayName("queueCommand creates defensive copy of args")
    void testQueueCommandCreatesDefensiveCopy() {
        context.startTransaction();
        List<String> args = Arrays.asList("key", "value");

        context.queueCommand(mockCommand, args);

        List<TransactionContext.QueuedCommand> queuedCommands = context.getQueuedCommands();
        List<String> queuedArgs = queuedCommands.get(0).args();

        // Queued args should be a different list (defensive copy)
        assertNotSame(args, queuedArgs);
        assertEquals(args, queuedArgs);
    }

    @Test
    @DisplayName("queueCommand can queue multiple commands")
    void testQueueMultipleCommands() {
        context.startTransaction();

        context.queueCommand(mockCommand, List.of("key1", "value1"));
        context.queueCommand(mockCommand, List.of("key2", "value2"));
        context.queueCommand(mockCommand, List.of("key3", "value3"));

        assertEquals(3, context.queueSize());
        List<TransactionContext.QueuedCommand> queuedCommands = context.getQueuedCommands();
        assertEquals(3, queuedCommands.size());
    }

    @Test
    @DisplayName("markError sets error flag")
    void testMarkError() {
        context.startTransaction();

        context.markError();

        assertTrue(context.hasErrors());
        assertTrue(context.isInTransaction());
    }

    @Test
    @DisplayName("discard clears queue and ends transaction")
    void testDiscard() {
        context.startTransaction();
        context.queueCommand(mockCommand, List.of("key", "value"));
        context.markError();

        boolean result = context.discard();

        assertTrue(result);
        assertFalse(context.isInTransaction());
        assertFalse(context.hasErrors());
        assertEquals(0, context.queueSize());
    }

    @Test
    @DisplayName("discard fails when not in transaction")
    void testDiscardWhenNotInTransaction() {
        boolean result = context.discard();

        assertFalse(result);
        assertFalse(context.isInTransaction());
    }

    @Test
    @DisplayName("endTransaction resets state")
    void testEndTransaction() {
        context.startTransaction();
        context.queueCommand(mockCommand, List.of("key", "value"));
        context.markError();

        context.endTransaction();

        assertFalse(context.isInTransaction());
        assertFalse(context.hasErrors());
        assertEquals(0, context.queueSize());
    }

    @Test
    @DisplayName("getOrCreate returns new context for channel")
    void testGetOrCreate() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);

        assertNotNull(ctx);
        assertFalse(ctx.isInTransaction());
    }

    @Test
    @DisplayName("getOrCreate returns same context on subsequent calls")
    void testGetOrCreateReturnsSameContext() {
        TransactionContext ctx1 = TransactionContext.getOrCreate(channel);
        TransactionContext ctx2 = TransactionContext.getOrCreate(channel);

        assertSame(ctx1, ctx2);
    }

    @Test
    @DisplayName("get returns null when no context exists")
    void testGetReturnsNullWhenNoContext() {
        TransactionContext ctx = TransactionContext.get(channel);

        assertNull(ctx);
    }

    @Test
    @DisplayName("get returns context after getOrCreate")
    void testGetReturnsContextAfterGetOrCreate() {
        TransactionContext ctx1 = TransactionContext.getOrCreate(channel);
        TransactionContext ctx2 = TransactionContext.get(channel);

        assertSame(ctx1, ctx2);
    }

    @Test
    @DisplayName("startTransaction clears previous queue")
    void testStartTransactionClearsPreviousQueue() {
        context.startTransaction();
        context.queueCommand(mockCommand, List.of("key1", "value1"));
        context.endTransaction();

        context.startTransaction();

        assertEquals(0, context.queueSize());
        assertFalse(context.hasErrors());
    }

    @Test
    @DisplayName("startTransaction resets error flag")
    void testStartTransactionResetsErrorFlag() {
        context.startTransaction();
        context.markError();
        context.endTransaction();

        context.startTransaction();

        assertFalse(context.hasErrors());
    }

    @Test
    @DisplayName("Queue can handle many commands")
    void testQueueCanHandleManyCommands() {
        context.startTransaction();

        // Queue 1000 commands
        for (int i = 0; i < 1000; i++) {
            context.queueCommand(mockCommand, List.of("key" + i, "value" + i));
        }

        assertEquals(1000, context.queueSize());
    }

    @Test
    @DisplayName("getQueuedCommands returns direct list reference")
    void testGetQueuedCommandsReturnsDirectReference() {
        context.startTransaction();
        context.queueCommand(mockCommand, List.of("key", "value"));

        List<TransactionContext.QueuedCommand> queue1 = context.getQueuedCommands();
        List<TransactionContext.QueuedCommand> queue2 = context.getQueuedCommands();

        // Should be same reference for performance
        assertSame(queue1, queue2);
    }

    @Test
    @DisplayName("Transaction state persists across multiple operations")
    void testTransactionStatePersistsAcrossOperations() {
        context.startTransaction();
        assertTrue(context.isInTransaction());

        context.queueCommand(mockCommand, List.of("key1", "value1"));
        assertTrue(context.isInTransaction());
        assertEquals(1, context.queueSize());

        context.queueCommand(mockCommand, List.of("key2", "value2"));
        assertTrue(context.isInTransaction());
        assertEquals(2, context.queueSize());

        context.markError();
        assertTrue(context.isInTransaction());
        assertTrue(context.hasErrors());
    }

    @Test
    @DisplayName("Discard allows starting new transaction")
    void testDiscardAllowsStartingNewTransaction() {
        context.startTransaction();
        context.queueCommand(mockCommand, List.of("key", "value"));
        context.discard();

        boolean result = context.startTransaction();

        assertTrue(result);
        assertTrue(context.isInTransaction());
        assertEquals(0, context.queueSize());
    }

    @Test
    @DisplayName("endTransaction allows starting new transaction")
    void testEndTransactionAllowsStartingNewTransaction() {
        context.startTransaction();
        context.queueCommand(mockCommand, List.of("key", "value"));
        context.endTransaction();

        boolean result = context.startTransaction();

        assertTrue(result);
        assertTrue(context.isInTransaction());
        assertEquals(0, context.queueSize());
    }

    @Test
    @DisplayName("Multiple errors can be marked")
    void testMultipleErrorsCanBeMarked() {
        context.startTransaction();

        context.markError();
        assertTrue(context.hasErrors());

        context.markError();
        assertTrue(context.hasErrors());

        // Error flag remains true
        assertTrue(context.hasErrors());
    }

    @Test
    @DisplayName("QueuedCommand record stores command and args")
    void testQueuedCommandRecord() {
        List<String> args = List.of("key", "value");
        TransactionContext.QueuedCommand qc = new TransactionContext.QueuedCommand(mockCommand, args);

        assertEquals(mockCommand, qc.command());
        assertEquals(args, qc.args());
    }

    @Test
    @DisplayName("Empty args list can be queued")
    void testEmptyArgsListCanBeQueued() {
        context.startTransaction();
        List<String> emptyArgs = List.of();

        context.queueCommand(mockCommand, emptyArgs);

        assertEquals(1, context.queueSize());
        List<TransactionContext.QueuedCommand> queuedCommands = context.getQueuedCommands();
        assertEquals(0, queuedCommands.get(0).args().size());
    }

    @Test
    @DisplayName("Context isolation between channels")
    void testContextIsolationBetweenChannels() {
        EmbeddedChannel channel1 = new EmbeddedChannel();
        EmbeddedChannel channel2 = new EmbeddedChannel();

        TransactionContext ctx1 = TransactionContext.getOrCreate(channel1);
        TransactionContext ctx2 = TransactionContext.getOrCreate(channel2);

        assertNotSame(ctx1, ctx2);

        ctx1.startTransaction();
        assertTrue(ctx1.isInTransaction());
        assertFalse(ctx2.isInTransaction());

        channel1.close();
        channel2.close();
    }

    @Test
    @DisplayName("Queue maintains insertion order")
    void testQueueMaintainsInsertionOrder() {
        context.startTransaction();

        ICommand cmd1 = mock(ICommand.class);
        ICommand cmd2 = mock(ICommand.class);
        ICommand cmd3 = mock(ICommand.class);

        context.queueCommand(cmd1, List.of("arg1"));
        context.queueCommand(cmd2, List.of("arg2"));
        context.queueCommand(cmd3, List.of("arg3"));

        List<TransactionContext.QueuedCommand> queue = context.getQueuedCommands();
        assertEquals(cmd1, queue.get(0).command());
        assertEquals(cmd2, queue.get(1).command());
        assertEquals(cmd3, queue.get(2).command());
    }
}
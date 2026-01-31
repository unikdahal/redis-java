package com.redis.commands.transaction;

import com.redis.commands.ICommand;
import com.redis.transaction.TransactionContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for EXEC command.
 *
 * Test Coverage:
 * - Basic EXEC execution
 * - EXEC without MULTI (error case)
 * - Empty transaction
 * - Transaction with errors (abort case)
 * - Command execution and response formatting
 * - State cleanup after execution
 */
@DisplayName("EXEC Command Unit Tests")
public class ExecCommandTest {

    private ExecCommand command;
    private ChannelHandlerContext mockCtx;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        command = new ExecCommand();
        channel = new EmbeddedChannel();
        mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.channel()).thenReturn(channel);
    }

    @Test
    @DisplayName("EXEC: Returns error when not in transaction")
    void testExecReturnsErrorWhenNotInTransaction() {
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("EXEC: Returns error when context doesn't exist")
    void testExecErrorWhenNoContext() {
        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("without MULTI"));
    }

    @Test
    @DisplayName("EXEC: Returns empty array for empty transaction")
    void testExecReturnsEmptyArrayForEmptyTransaction() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertEquals("*0\r\n", result);
        assertFalse(ctx.isInTransaction());
    }

    @Test
    @DisplayName("EXEC: Returns abort error when hasErrors is true")
    void testExecReturnsAbortWhenHasErrors() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();
        ctx.markError();

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.contains("EXECABORT") || result.contains("discarded"));
        assertFalse(ctx.isInTransaction());
    }

    @Test
    @DisplayName("EXEC: Executes single queued command")
    void testExecExecutesSingleCommand() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand mockCommand = mock(ICommand.class);
        when(mockCommand.execute(any(), eq(mockCtx))).thenReturn("+OK\r\n");

        ctx.queueCommand(mockCommand, Collections.emptyList());

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*1\r\n"));
        assertTrue(result.contains("+OK\r\n"));
        assertFalse(ctx.isInTransaction());
    }

    @Test
    @DisplayName("EXEC: Executes multiple queued commands")
    void testExecExecutesMultipleCommands() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand cmd1 = mock(ICommand.class);
        ICommand cmd2 = mock(ICommand.class);
        ICommand cmd3 = mock(ICommand.class);

        when(cmd1.execute(any(), eq(mockCtx))).thenReturn("+OK\r\n");
        when(cmd2.execute(any(), eq(mockCtx))).thenReturn(":42\r\n");
        when(cmd3.execute(any(), eq(mockCtx))).thenReturn("$5\r\nhello\r\n");

        ctx.queueCommand(cmd1, Collections.emptyList());
        ctx.queueCommand(cmd2, Collections.emptyList());
        ctx.queueCommand(cmd3, Collections.emptyList());

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*3\r\n"));
        assertTrue(result.contains("+OK\r\n"));
        assertTrue(result.contains(":42\r\n"));
        assertTrue(result.contains("$5\r\nhello\r\n"));
    }

    @Test
    @DisplayName("EXEC: Handles command returning null")
    void testExecHandlesCommandReturningNull() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand mockCommand = mock(ICommand.class);
        when(mockCommand.execute(any(), eq(mockCtx))).thenReturn(null);

        ctx.queueCommand(mockCommand, Collections.emptyList());

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*1\r\n"));
        assertTrue(result.contains("$-1\r\n")); // Nil reply for null
    }

    @Test
    @DisplayName("EXEC: Handles command throwing exception")
    void testExecHandlesCommandThrowingException() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand mockCommand = mock(ICommand.class);
        when(mockCommand.execute(any(), eq(mockCtx))).thenThrow(new RuntimeException("Test error"));

        ctx.queueCommand(mockCommand, Collections.emptyList());

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*1\r\n"));
        assertTrue(result.contains("-ERR"));
        assertTrue(result.contains("Test error"));
    }

    @Test
    @DisplayName("EXEC: Command name is EXEC")
    void testCommandName() {
        assertEquals("EXEC", command.name());
    }

    @Test
    @DisplayName("EXEC: Rejects extra arguments")
    void testExecRejectsArguments() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        String result = command.execute(Collections.singletonList("extra"), mockCtx);

        assertTrue(result.startsWith("-ERR"), "EXEC should reject extra arguments");
    }

    @Test
    @DisplayName("EXEC: Ends transaction after execution")
    void testExecEndsTransaction() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand mockCommand = mock(ICommand.class);
        when(mockCommand.execute(any(), eq(mockCtx))).thenReturn("+OK\r\n");
        ctx.queueCommand(mockCommand, Collections.emptyList());

        command.execute(Collections.emptyList(), mockCtx);

        assertFalse(ctx.isInTransaction());
        assertEquals(0, ctx.queueSize());
    }

    @Test
    @DisplayName("EXEC: Can start new transaction after exec")
    void testCanStartNewTransactionAfterExec() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand mockCommand = mock(ICommand.class);
        when(mockCommand.execute(any(), eq(mockCtx))).thenReturn("+OK\r\n");
        ctx.queueCommand(mockCommand, Collections.emptyList());

        command.execute(Collections.emptyList(), mockCtx);

        // Should be able to start new transaction
        boolean result = ctx.startTransaction();
        assertTrue(result);
    }

    @Test
    @DisplayName("EXEC: Commands execute in order")
    void testCommandsExecuteInOrder() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand cmd1 = mock(ICommand.class);
        ICommand cmd2 = mock(ICommand.class);
        ICommand cmd3 = mock(ICommand.class);

        when(cmd1.execute(any(), eq(mockCtx))).thenReturn("+FIRST\r\n");
        when(cmd2.execute(any(), eq(mockCtx))).thenReturn("+SECOND\r\n");
        when(cmd3.execute(any(), eq(mockCtx))).thenReturn("+THIRD\r\n");

        ctx.queueCommand(cmd1, Collections.emptyList());
        ctx.queueCommand(cmd2, Collections.emptyList());
        ctx.queueCommand(cmd3, Collections.emptyList());

        String result = command.execute(Collections.emptyList(), mockCtx);

        // Check order by finding indices
        int firstIndex = result.indexOf("FIRST");
        int secondIndex = result.indexOf("SECOND");
        int thirdIndex = result.indexOf("THIRD");

        assertTrue(firstIndex < secondIndex);
        assertTrue(secondIndex < thirdIndex);
    }

    @Test
    @DisplayName("EXEC: Executes large transaction")
    void testExecExecutesLargeTransaction() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        for (int i = 0; i < 100; i++) {
            ICommand mockCommand = mock(ICommand.class);
            when(mockCommand.execute(any(), eq(mockCtx))).thenReturn("+OK\r\n");
            ctx.queueCommand(mockCommand, Collections.emptyList());
        }

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*100\r\n"));
        assertFalse(ctx.isInTransaction());
    }

    @Test
    @DisplayName("EXEC: Mixed success and error commands")
    void testExecMixedSuccessAndErrorCommands() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand successCmd = mock(ICommand.class);
        ICommand errorCmd = mock(ICommand.class);

        when(successCmd.execute(any(), eq(mockCtx))).thenReturn("+OK\r\n");
        when(errorCmd.execute(any(), eq(mockCtx))).thenThrow(new RuntimeException("Error"));

        ctx.queueCommand(successCmd, Collections.emptyList());
        ctx.queueCommand(errorCmd, Collections.emptyList());
        ctx.queueCommand(successCmd, Collections.emptyList());

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*3\r\n"));
        assertTrue(result.contains("+OK\r\n"));
        assertTrue(result.contains("-ERR"));
    }

    @Test
    @DisplayName("EXEC: Clears error flag after abort")
    void testExecClearsErrorFlagAfterAbort() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();
        ctx.markError();

        command.execute(Collections.emptyList(), mockCtx);

        assertFalse(ctx.hasErrors());
    }

    @Test
    @DisplayName("EXEC: Context persists after execution")
    void testContextPersistsAfterExecution() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        command.execute(Collections.emptyList(), mockCtx);

        TransactionContext afterExec = TransactionContext.get(channel);
        assertSame(ctx, afterExec);
    }

    @Test
    @DisplayName("EXEC: Commands with arguments execute correctly")
    void testCommandsWithArgumentsExecuteCorrectly() {
        TransactionContext ctx = TransactionContext.getOrCreate(channel);
        ctx.startTransaction();

        ICommand mockCommand = mock(ICommand.class);
        List<String> args = List.of("key", "value");
        when(mockCommand.execute(eq(args), eq(mockCtx))).thenReturn("+OK\r\n");

        ctx.queueCommand(mockCommand, args);

        String result = command.execute(Collections.emptyList(), mockCtx);

        assertTrue(result.startsWith("*1\r\n"));
        assertTrue(result.contains("+OK\r\n"));
    }
}
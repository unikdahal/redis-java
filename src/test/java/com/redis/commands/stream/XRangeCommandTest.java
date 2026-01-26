package com.redis.commands.stream;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class XRangeCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private XRangeCommand command;
    private XAddCommand addCommand;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new XRangeCommand();
        addCommand = new XAddCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testXRange_Basic() {
        db.remove("rangestream");
        addCommand.execute(List.of("rangestream", "1000-0", "a", "1"), ctx);
        addCommand.execute(List.of("rangestream", "1000-1", "b", "2"), ctx);
        addCommand.execute(List.of("rangestream", "1001-0", "c", "3"), ctx);

        String result = command.execute(List.of("rangestream", "1000-0", "1000-1"), ctx);
        assertTrue(result.startsWith("*2"));
        assertTrue(result.contains("1000-0"));
        assertTrue(result.contains("1000-1"));
        assertFalse(result.contains("1001-0"));
        db.remove("rangestream");
    }

    @Test
    void testXRange_SpecialBounds() {
        db.remove("rangestream2");
        addCommand.execute(List.of("rangestream2", "1000-0", "a", "1"), ctx);
        addCommand.execute(List.of("rangestream2", "2000-0", "b", "2"), ctx);

        String result = command.execute(List.of("rangestream2", "-", "+"), ctx);
        assertTrue(result.startsWith("*2"));
        assertTrue(result.contains("1000-0"));
        assertTrue(result.contains("2000-0"));
        db.remove("rangestream2");
    }

    @Test
    void testXRange_WithCount() {
        db.remove("rangestream3");
        for (int i = 0; i < 10; i++) {
            addCommand.execute(List.of("rangestream3", (1000 + i) + "-0", "val", String.valueOf(i)), ctx);
        }

        String result = command.execute(List.of("rangestream3", "-", "+", "COUNT", "3"), ctx);
        assertTrue(result.startsWith("*3"));
        db.remove("rangestream3");
    }
}

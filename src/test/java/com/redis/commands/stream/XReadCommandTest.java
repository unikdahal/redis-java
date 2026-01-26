package com.redis.commands.stream;

import com.redis.storage.RedisDatabase;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class XReadCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private XReadCommand command;
    private XAddCommand addCommand;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new XReadCommand();
        addCommand = new XAddCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testXRead_Basic() {
        db.remove("readstream");
        addCommand.execute(List.of("readstream", "1000-0", "f1", "v1"), ctx);
        addCommand.execute(List.of("readstream", "1000-1", "f2", "v2"), ctx);

        String result = command.execute(List.of("STREAMS", "readstream", "1000-0"), ctx);
        assertTrue(result.startsWith("*1")); // 1 stream
        assertTrue(result.contains("readstream"));
        assertTrue(result.contains("1000-1"));
        assertFalse(result.contains("1000-0"));
        db.remove("readstream");
    }

    @Test
    void testXRead_MultipleStreams() {
        db.remove("s1");
        db.remove("s2");
        addCommand.execute(List.of("s1", "1000-0", "a", "1"), ctx);
        addCommand.execute(List.of("s2", "2000-0", "b", "2"), ctx);

        String result = command.execute(List.of("STREAMS", "s1", "s2", "0-0", "0-0"), ctx);
        assertTrue(result.startsWith("*2"));
        assertTrue(result.contains("s1"));
        assertTrue(result.contains("s2"));
        db.remove("s1");
        db.remove("s2");
    }

    @Test
    void testXRead_NoDataReturnsNil() {
        db.remove("empty_stream");
        addCommand.execute(List.of("empty_stream", "1000-0", "f", "v"), ctx);
        
        String result = command.execute(List.of("STREAMS", "empty_stream", "1000-0"), ctx);
        assertEquals("*-1\r\n", result);
        db.remove("empty_stream");
    }
}

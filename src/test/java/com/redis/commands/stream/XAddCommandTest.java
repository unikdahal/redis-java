package com.redis.commands.stream;

import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import com.redis.util.StreamId;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class XAddCommandTest {

    @Mock
    private ChannelHandlerContext ctx;

    private XAddCommand command;
    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        command = new XAddCommand();
        db = RedisDatabase.getInstance();
    }

    @Test
    void testXAdd_AutoId() {
        db.remove("mystream_auto");
        String result = command.execute(List.of("mystream_auto", "*", "field1", "value1"), ctx);
        assertTrue(result.startsWith("$"));
        
        RedisValue value = db.getValue("mystream_auto");
        assertNotNull(value);
        assertEquals(RedisValue.Type.STREAM, value.getType());
        
        Map<StreamId, Map<String, String>> stream = value.asStream();
        assertEquals(1, stream.size());
        Map.Entry<StreamId, Map<String, String>> entry = stream.entrySet().iterator().next();
        assertEquals("value1", entry.getValue().get("field1"));
        db.remove("mystream_auto");
    }

    @Test
    void testXAdd_SpecificId() {
        db.remove("mystream_spec");
        String result = command.execute(List.of("mystream_spec", "1518713280000-0", "f1", "v1"), ctx);
        assertEquals("$15\r\n1518713280000-0\r\n", result);
        
        StreamId id = db.getValue("mystream_spec").asStream().keySet().iterator().next();
        assertEquals(1518713280000L, id.time());
        assertEquals(0L, id.sequence());
        db.remove("mystream_spec");
    }

    @Test
    void testXAdd_ErrorTooSmallId() {
        db.remove("mystream_err");
        command.execute(List.of("mystream_err", "1000-10", "f", "v"), ctx);
        String result = command.execute(List.of("mystream_err", "1000-9", "f", "v"), ctx);
        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("smaller"));
        db.remove("mystream_err");
    }

    @Test
    void testXAdd_ZeroIdError() {
        db.remove("mystream_zero");
        String result = command.execute(List.of("mystream_zero", "0-0", "f", "v"), ctx);
        assertTrue(result.contains("ERR"));
        assertTrue(result.contains("greater than 0-0"));
        db.remove("mystream_zero");
    }
}

package com.redis.server;

import com.redis.commands.CommandRegistry;
import com.redis.commands.GetCommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for RedisCommandHandler WRONGTYPE exception handling.
 * Verifies that WRONGTYPE errors are properly converted to RESP error responses.
 */
@DisplayName("RedisCommandHandler WRONGTYPE Error Handling")
public class RedisCommandHandlerWrongTypeTest {

    private RedisDatabase db;
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        db = RedisDatabase.getInstance();
        
        // Register GET command
        CommandRegistry.getInstance().register(new GetCommand());
        
        // Create embedded channel with handler
        channel = new EmbeddedChannel(new RedisCommandHandler());
    }

    @Test
    @DisplayName("GET on LIST value returns WRONGTYPE error in RESP format")
    void testGetOnListReturnsWrongTypeError() {
        // Store a LIST value
        db.put("mylist", RedisValue.list(List.of("item1", "item2")));

        // Send GET command for the list key
        String command = "*2\r\n$3\r\nGET\r\n$6\r\nmylist\r\n";
        ByteBuf request = Unpooled.copiedBuffer(command, StandardCharsets.UTF_8);
        
        channel.writeInbound(request);
        
        // Read the response
        ByteBuf response = channel.readOutbound();
        assertNotNull(response, "Response should not be null");
        
        String responseStr = response.toString(StandardCharsets.UTF_8);
        response.release();
        
        // Verify RESP error format: -WRONGTYPE ...\r\n
        assertTrue(responseStr.startsWith("-WRONGTYPE"), 
            "Response should start with -WRONGTYPE, got: " + responseStr);
        assertTrue(responseStr.endsWith("\r\n"), 
            "Response should end with \\r\\n");
        assertTrue(responseStr.contains("LIST"), 
            "Error message should mention LIST type");
    }

    @Test
    @DisplayName("GET on SET value returns WRONGTYPE error in RESP format")
    void testGetOnSetReturnsWrongTypeError() {
        // Store a SET value
        db.put("myset", RedisValue.set(java.util.Set.of("member1", "member2")));

        // Send GET command for the set key
        String command = "*2\r\n$3\r\nGET\r\n$5\r\nmyset\r\n";
        ByteBuf request = Unpooled.copiedBuffer(command, StandardCharsets.UTF_8);
        
        channel.writeInbound(request);
        
        // Read the response
        ByteBuf response = channel.readOutbound();
        assertNotNull(response, "Response should not be null");
        
        String responseStr = response.toString(StandardCharsets.UTF_8);
        response.release();
        
        // Verify RESP error format
        assertTrue(responseStr.startsWith("-WRONGTYPE"), 
            "Response should start with -WRONGTYPE, got: " + responseStr);
        assertTrue(responseStr.endsWith("\r\n"), 
            "Response should end with \\r\\n");
        assertTrue(responseStr.contains("SET"), 
            "Error message should mention SET type");
    }

    @Test
    @DisplayName("GET on HASH value returns WRONGTYPE error in RESP format")
    void testGetOnHashReturnsWrongTypeError() {
        // Store a HASH value
        db.put("myhash", RedisValue.hash(java.util.Map.of("field1", "value1")));

        // Send GET command for the hash key
        String command = "*2\r\n$3\r\nGET\r\n$6\r\nmyhash\r\n";
        ByteBuf request = Unpooled.copiedBuffer(command, StandardCharsets.UTF_8);
        
        channel.writeInbound(request);
        
        // Read the response
        ByteBuf response = channel.readOutbound();
        assertNotNull(response, "Response should not be null");
        
        String responseStr = response.toString(StandardCharsets.UTF_8);
        response.release();
        
        // Verify RESP error format
        assertTrue(responseStr.startsWith("-WRONGTYPE"), 
            "Response should start with -WRONGTYPE, got: " + responseStr);
        assertTrue(responseStr.endsWith("\r\n"), 
            "Response should end with \\r\\n");
        assertTrue(responseStr.contains("HASH"), 
            "Error message should mention HASH type");
    }

    @Test
    @DisplayName("GET on STRING value returns success (not WRONGTYPE)")
    void testGetOnStringReturnsSuccess() {
        // Store a STRING value
        db.put("mystring", "hello");

        // Send GET command for the string key
        String command = "*2\r\n$3\r\nGET\r\n$8\r\nmystring\r\n";
        ByteBuf request = Unpooled.copiedBuffer(command, StandardCharsets.UTF_8);
        
        channel.writeInbound(request);
        
        // Read the response
        ByteBuf response = channel.readOutbound();
        assertNotNull(response, "Response should not be null");
        
        String responseStr = response.toString(StandardCharsets.UTF_8);
        response.release();
        
        // Verify successful RESP bulk string format: $<len>\r\n<value>\r\n
        assertTrue(responseStr.startsWith("$"), 
            "Response should start with $, got: " + responseStr);
        assertTrue(responseStr.contains("hello"), 
            "Response should contain the value");
        assertFalse(responseStr.contains("WRONGTYPE"), 
            "Response should not contain WRONGTYPE error");
    }
}

package com.redis.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RedisValue wrapper class.
 */
class RedisValueTest {

    @Test
    void testStringValue() {
        RedisValue value = RedisValue.string("hello");

        assertEquals(RedisValue.Type.STRING, value.getType());
        assertEquals("hello", value.asString());
        assertTrue(value.isType(RedisValue.Type.STRING));
        assertFalse(value.isType(RedisValue.Type.LIST));
    }

    @Test
    void testListValue() {
        List<String> list = List.of("a", "b", "c");
        RedisValue value = RedisValue.list(list);

        assertEquals(RedisValue.Type.LIST, value.getType());
        assertEquals(list, value.asList());
        assertTrue(value.isType(RedisValue.Type.LIST));
    }

    @Test
    void testSetValue() {
        Set<String> set = Set.of("x", "y", "z");
        RedisValue value = RedisValue.set(set);

        assertEquals(RedisValue.Type.SET, value.getType());
        assertEquals(set, value.asSet());
        assertTrue(value.isType(RedisValue.Type.SET));
    }

    @Test
    void testHashValue() {
        Map<String, String> hash = Map.of("field1", "value1", "field2", "value2");
        RedisValue value = RedisValue.hash(hash);

        assertEquals(RedisValue.Type.HASH, value.getType());
        assertEquals(hash, value.asHash());
        assertTrue(value.isType(RedisValue.Type.HASH));
    }

    @Test
    void testWrongTypeAccessThrows() {
        RedisValue stringValue = RedisValue.string("test");

        assertThrows(IllegalStateException.class, stringValue::asList);
        assertThrows(IllegalStateException.class, stringValue::asSet);
        assertThrows(IllegalStateException.class, stringValue::asHash);
    }

    @Test
    void testWrongTypeErrorMessage() {
        RedisValue listValue = RedisValue.list(List.of("a"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, listValue::asString);
        assertTrue(ex.getMessage().contains("WRONGTYPE"));
        assertTrue(ex.getMessage().contains("LIST"));
    }

    @Test
    void testEquality() {
        RedisValue v1 = RedisValue.string("test");
        RedisValue v2 = RedisValue.string("test");
        RedisValue v3 = RedisValue.string("other");

        assertEquals(v1, v2);
        assertNotEquals(v1, v3);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    void testDifferentTypesNotEqual() {
        RedisValue stringVal = RedisValue.string("test");
        RedisValue listVal = RedisValue.list(List.of("test"));

        assertNotEquals(stringVal, listVal);
    }

    @Test
    void testGetData() {
        String data = "mydata";
        RedisValue value = RedisValue.string(data);

        assertSame(data, value.getData());
    }

    @Test
    void testToString() {
        RedisValue value = RedisValue.string("hello");
        String str = value.toString();

        assertTrue(str.contains("STRING"));
        assertTrue(str.contains("hello"));
    }

    @Test
    void testListDefensiveCopy() {
        java.util.ArrayList<String> mutableList = new java.util.ArrayList<>();
        mutableList.add("a");
        mutableList.add("b");

        RedisValue value = RedisValue.list(mutableList);

        // Modify the original list
        mutableList.add("c");

        // The RedisValue should not be affected
        assertEquals(2, value.asList().size());
        assertEquals(List.of("a", "b"), value.asList());
    }

    @Test
    void testSetDefensiveCopy() {
        java.util.HashSet<String> mutableSet = new java.util.HashSet<>();
        mutableSet.add("x");
        mutableSet.add("y");

        RedisValue value = RedisValue.set(mutableSet);

        // Modify the original set
        mutableSet.add("z");

        // The RedisValue should not be affected
        assertEquals(2, value.asSet().size());
        assertTrue(value.asSet().contains("x"));
        assertTrue(value.asSet().contains("y"));
        assertFalse(value.asSet().contains("z"));
    }

    @Test
    void testHashDefensiveCopy() {
        java.util.HashMap<String, String> mutableMap = new java.util.HashMap<>();
        mutableMap.put("key1", "value1");
        mutableMap.put("key2", "value2");

        RedisValue value = RedisValue.hash(mutableMap);

        // Modify the original map
        mutableMap.put("key3", "value3");

        // The RedisValue should not be affected
        assertEquals(2, value.asHash().size());
        assertEquals("value1", value.asHash().get("key1"));
        assertEquals("value2", value.asHash().get("key2"));
        assertNull(value.asHash().get("key3"));
    }
}

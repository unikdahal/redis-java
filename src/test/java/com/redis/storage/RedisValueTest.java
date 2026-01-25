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
    void testListImmutability() {
        List<String> list = List.of("a", "b", "c");
        RedisValue value = RedisValue.list(list);
        List<String> retrievedList = value.asList();

        // Verify that attempting to modify the list throws UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> retrievedList.add("d"));
        assertThrows(UnsupportedOperationException.class, () -> retrievedList.remove(0));
        assertThrows(UnsupportedOperationException.class, () -> retrievedList.clear());
    }

    @Test
    void testSetImmutability() {
        Set<String> set = Set.of("x", "y", "z");
        RedisValue value = RedisValue.set(set);
        Set<String> retrievedSet = value.asSet();

        // Verify that attempting to modify the set throws UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> retrievedSet.add("w"));
        assertThrows(UnsupportedOperationException.class, () -> retrievedSet.remove("x"));
        assertThrows(UnsupportedOperationException.class, () -> retrievedSet.clear());
    }

    @Test
    void testHashImmutability() {
        Map<String, String> hash = Map.of("field1", "value1", "field2", "value2");
        RedisValue value = RedisValue.hash(hash);
        Map<String, String> retrievedHash = value.asHash();

        // Verify that attempting to modify the map throws UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> retrievedHash.put("field3", "value3"));
        assertThrows(UnsupportedOperationException.class, () -> retrievedHash.remove("field1"));
        assertThrows(UnsupportedOperationException.class, () -> retrievedHash.clear());
    }
}

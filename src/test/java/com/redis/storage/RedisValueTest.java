package com.redis.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RedisValue wrapper class.
 */
class RedisValueTest {

    /**
     * Helper method to create a RedisValue using reflection for testing validation.
     * This bypasses the factory methods to directly test the private constructor.
     */
    private RedisValue createViaReflection(RedisValue.Type type, Object data) throws Exception {
        var constructor = RedisValue.class.getDeclaredConstructor(RedisValue.Type.class, Object.class);
        constructor.setAccessible(true);
        return (RedisValue) constructor.newInstance(type, data);
    }

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
    void testSortedSetValue() {
        Map<String, Double> sortedSet = Map.of("member1", 1.0, "member2", 2.5, "member3", 3.0);
        RedisValue value = RedisValue.sortedSet(sortedSet);

        assertEquals(RedisValue.Type.SORTED_SET, value.getType());
        assertEquals(sortedSet, value.asSortedSet());
        assertTrue(value.isType(RedisValue.Type.SORTED_SET));
    }

    @Test
    void testWrongTypeAccessThrows() {
        RedisValue stringValue = RedisValue.string("test");

        assertThrows(RedisWrongTypeException.class, stringValue::asList);
        assertThrows(RedisWrongTypeException.class, stringValue::asSet);
        assertThrows(RedisWrongTypeException.class, stringValue::asHash);
    }

    @Test
    void testWrongTypeErrorMessage() {
        RedisValue listValue = RedisValue.list(List.of("a"));

        RedisWrongTypeException ex = assertThrows(RedisWrongTypeException.class, listValue::asString);
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
    void testListImmutability() {
        List<String> original = new ArrayList<>(List.of("a", "b", "c"));
        RedisValue value = RedisValue.list(original);

        // Modifying the original list should not affect the RedisValue
        original.add("d");
        assertEquals(3, value.asList().size());
        assertFalse(value.asList().contains("d"));

        // The returned list from asList() should be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> value.asList().add("e"));
        assertThrows(UnsupportedOperationException.class, () -> value.asList().remove(0));
        assertThrows(UnsupportedOperationException.class, () -> value.asList().clear());
    }

    @Test
    void testSetImmutability() {
        Set<String> original = new HashSet<>(Set.of("x", "y", "z"));
        RedisValue value = RedisValue.set(original);

        // Modifying the original set should not affect the RedisValue
        original.add("w");
        assertEquals(3, value.asSet().size());
        assertFalse(value.asSet().contains("w"));

        // The returned set from asSet() should be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> value.asSet().add("v"));
        assertThrows(UnsupportedOperationException.class, () -> value.asSet().remove("x"));
        assertThrows(UnsupportedOperationException.class, () -> value.asSet().clear());
    }

    @Test
    void testHashImmutability() {
        Map<String, String> original = new HashMap<>(Map.of("key1", "val1", "key2", "val2"));
        RedisValue value = RedisValue.hash(original);

        // Modifying the original map should not affect the RedisValue
        original.put("key3", "val3");
        assertEquals(2, value.asHash().size());
        assertFalse(value.asHash().containsKey("key3"));

        // The returned map from asHash() should be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> value.asHash().put("key4", "val4"));
        assertThrows(UnsupportedOperationException.class, () -> value.asHash().remove("key1"));
        assertThrows(UnsupportedOperationException.class, () -> value.asHash().clear());
    }

    @Test
    void testSortedSetImmutability() {
        Map<String, Double> original = new HashMap<>(Map.of("member1", 1.0, "member2", 2.0));
        RedisValue value = RedisValue.sortedSet(original);

        // Modifying the original map should not affect the RedisValue
        original.put("member3", 3.0);
        assertEquals(2, value.asSortedSet().size());
        assertFalse(value.asSortedSet().containsKey("member3"));

        // The returned map from asSortedSet() should be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> value.asSortedSet().put("member4", 4.0));
        assertThrows(UnsupportedOperationException.class, () -> value.asSortedSet().remove("member1"));
        assertThrows(UnsupportedOperationException.class, () -> value.asSortedSet().clear());
    }

    @Test
    void testToString() {
        RedisValue value = RedisValue.string("hello");
        String str = value.toString();

        assertTrue(str.contains("STRING"));
        assertTrue(str.contains("hello"));
    }

    @Test
    void testNullDataThrows() {
        // Use reflection to access the private constructor for testing
        assertThrows(Exception.class, () -> createViaReflection(RedisValue.Type.STRING, null));
    }

    @Test
    void testMismatchedTypeThrows() {
        // Use reflection to test type validation
        
        // Try to create a STRING type with a List data
        assertThrows(Exception.class, () -> createViaReflection(RedisValue.Type.STRING, List.of("test")));

        // Try to create a LIST type with a String data
        assertThrows(Exception.class, () -> createViaReflection(RedisValue.Type.LIST, "test"));

        // Try to create a SET type with a List data
        assertThrows(Exception.class, () -> createViaReflection(RedisValue.Type.SET, List.of("test")));

        // Try to create a HASH type with a String data
        assertThrows(Exception.class, () -> createViaReflection(RedisValue.Type.HASH, "test"));
    }
}

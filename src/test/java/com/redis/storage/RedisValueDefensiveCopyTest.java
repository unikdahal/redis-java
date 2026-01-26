package com.redis.storage;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Set;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RedisValueDefensiveCopyTest {

    @Test
    void testListDefensiveCopyAndMutablity() {
        List<String> immutableList = List.of("a", "b");
        RedisValue value = RedisValue.list(immutableList);
        List<String> storedList = value.asList();

        // Should be a different instance
        assertNotSame(immutableList, storedList);

        // Should be mutable (this currently fails if it's just a reference to List.of)
        try {
            storedList.add("c");
        } catch (UnsupportedOperationException e) {
            fail("Stored list should be mutable");
        }
        assertEquals(3, storedList.size());
    }

    @Test
    void testSetDefensiveCopyAndMutability() {
        Set<String> immutableSet = Set.of("a", "b");
        RedisValue value = RedisValue.set(immutableSet);
        Set<String> storedSet = value.asSet();

        assertNotSame(immutableSet, storedSet);

        try {
            storedSet.add("c");
        } catch (UnsupportedOperationException e) {
            fail("Stored set should be mutable");
        }
        assertEquals(3, storedSet.size());
    }

    @Test
    void testHashDefensiveCopyAndMutability() {
        Map<String, String> immutableMap = Map.of("k1", "v1");
        RedisValue value = RedisValue.hash(immutableMap);
        Map<String, String> storedMap = value.asHash();

        assertNotSame(immutableMap, storedMap);

        try {
            storedMap.put("k2", "v2");
        } catch (UnsupportedOperationException e) {
            fail("Stored hash should be mutable");
        }
        assertEquals(2, storedMap.size());
    }

    @Test
    void testSortedSetDefensiveCopyAndMutability() {
        Map<String, Double> immutableMap = Map.of("m1", 1.0);
        RedisValue value = RedisValue.sortedSet(immutableMap);
        Map<String, Double> storedMap = value.asSortedSet();

        assertNotSame(immutableMap, storedMap);

        try {
            storedMap.put("m2", 2.0);
        } catch (UnsupportedOperationException e) {
            fail("Stored sorted set should be mutable");
        }
        assertEquals(2, storedMap.size());
    }
}

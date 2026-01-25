package com.redis.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for RedisDatabase
 */
@DisplayName("RedisDatabase Unit Tests")
public class RedisDatabaseTest {

    private RedisDatabase db;

    @BeforeEach
    void setUp() {
        db = RedisDatabase.getInstance();
        // Note: In production, would need a clear/reset method
    }

    @Test
    @DisplayName("Database: Put and Get basic operation")
    void testPutAndGet() {
        db.put("test_key", "test_value");
        String result = db.get("test_key");
        assertEquals("test_value", result);
    }

    @Test
    @DisplayName("Database: Get non-existent key returns null")
    void testGetNonExistent() {
        String result = db.get("nonexistent_xyz");
        assertNull(result);
    }

    @Test
    @DisplayName("Database: Overwrite existing key")
    void testOverwrite() {
        db.put("key", "v1");
        db.put("key", "v2");
        assertEquals("v2", db.get("key"));
    }

    @Test
    @DisplayName("Database: Put with TTL in milliseconds")
    void testPutWithTtl() {
        db.put("ttl_key", "value", 1000);
        String result = db.get("ttl_key");
        assertEquals("value", result);
    }

    @Test
    @DisplayName("Database: Expired key returns null")
    void testExpiredKey() throws InterruptedException {
        db.put("exp_key", "value", 100);  // 100ms TTL
        assertEquals("value", db.get("exp_key"));

        Thread.sleep(150);
        assertNull(db.get("exp_key"));
    }

    @Test
    @DisplayName("Database: Exists on existing key")
    void testExistsTrue() {
        db.put("exist_key", "value");
        assertTrue(db.exists("exist_key"));
    }

    @Test
    @DisplayName("Database: Exists on non-existent key")
    void testExistsFalse() {
        assertFalse(db.exists("nonexist_key"));
    }

    @Test
    @DisplayName("Database: Exists on expired key returns false")
    void testExistsExpired() throws InterruptedException {
        db.put("exp_exist", "value", 100);
        assertTrue(db.exists("exp_exist"));

        Thread.sleep(150);
        assertFalse(db.exists("exp_exist"));
    }

    @Test
    @DisplayName("Database: Remove existing key")
    void testRemoveExisting() {
        db.put("remove_key", "value");
        assertTrue(db.remove("remove_key"));
        assertNull(db.get("remove_key"));
    }

    @Test
    @DisplayName("Database: Remove non-existent key")
    void testRemoveNonExistent() {
        assertFalse(db.remove("nonexist_remove"));
    }

    @Test
    @DisplayName("Database: Remove all keys")
    void testRemoveAll() {
        db.put("rm1", "v1");
        db.put("rm2", "v2");
        db.put("rm3", "v3");

        Collection<String> keys = Arrays.asList("rm1", "rm2", "rm3");
        int count = db.removeAll(keys);
        assertEquals(3, count);

        assertNull(db.get("rm1"));
        assertNull(db.get("rm2"));
        assertNull(db.get("rm3"));
    }

    @Test
    @DisplayName("Database: Remove all with non-existent keys")
    void testRemoveAllMixed() {
        db.put("exist", "value");
        Collection<String> keys = Arrays.asList("exist", "nonexist");
        int count = db.removeAll(keys);
        assertEquals(1, count);
    }

    @Test
    @DisplayName("Database: Size counter")
    void testSize() {
        int initial = db.size();
        db.put("size1", "v1");
        db.put("size2", "v2");
        int after = db.size();
        assertTrue(after >= initial + 2);
    }

    @Test
    @DisplayName("Database: Large value storage")
    void testLargeValue() {
        String largeValue = "x".repeat(100000);
        db.put("large", largeValue);
        assertEquals(largeValue, db.get("large"));
    }

    @Test
    @DisplayName("Database: Empty value storage")
    void testEmptyValue() {
        db.put("empty", "");
        assertEquals("", db.get("empty"));
    }

    @Test
    @DisplayName("Database: Special characters in value")
    void testSpecialChars() {
        String special = "val\r\nue:with*special[chars]";
        db.put("special", special);
        assertEquals(special, db.get("special"));
    }

    @Test
    @DisplayName("Database: Concurrent put and get")
    void testConcurrentOperations() throws InterruptedException {
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                db.put("concurrent_" + i, "value_" + i);
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                Thread.sleep(50);
                for (int i = 0; i < 100; i++) {
                    String val = db.get("concurrent_" + i);
                    if (val != null) {
                        assertTrue(val.startsWith("value_"));
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    // ==================== Tests for getValue() ====================

    @Test
    @DisplayName("getValue: Returns RedisValue for STRING type")
    void testGetValueString() {
        db.put("str_key", RedisValue.string("test_value"));
        RedisValue value = db.getValue("str_key");
        
        assertNotNull(value);
        assertEquals(RedisValue.Type.STRING, value.getType());
        assertEquals("test_value", value.asString());
    }

    @Test
    @DisplayName("getValue: Returns RedisValue for LIST type")
    void testGetValueList() {
        List<String> list = Arrays.asList("a", "b", "c");
        db.put("list_key", RedisValue.list(list));
        RedisValue value = db.getValue("list_key");
        
        assertNotNull(value);
        assertEquals(RedisValue.Type.LIST, value.getType());
        assertEquals(list, value.asList());
    }

    @Test
    @DisplayName("getValue: Returns RedisValue for SET type")
    void testGetValueSet() {
        Set<String> set = new HashSet<>(Arrays.asList("x", "y", "z"));
        db.put("set_key", RedisValue.set(set));
        RedisValue value = db.getValue("set_key");
        
        assertNotNull(value);
        assertEquals(RedisValue.Type.SET, value.getType());
        assertEquals(set, value.asSet());
    }

    @Test
    @DisplayName("getValue: Returns RedisValue for HASH type")
    void testGetValueHash() {
        Map<String, String> hash = new HashMap<>();
        hash.put("field1", "value1");
        hash.put("field2", "value2");
        db.put("hash_key", RedisValue.hash(hash));
        RedisValue value = db.getValue("hash_key");
        
        assertNotNull(value);
        assertEquals(RedisValue.Type.HASH, value.getType());
        assertEquals(hash, value.asHash());
    }

    @Test
    @DisplayName("getValue: Returns null for non-existent key")
    void testGetValueNonExistent() {
        RedisValue value = db.getValue("nonexistent_getValue");
        assertNull(value);
    }

    @Test
    @DisplayName("getValue: Returns null for expired key")
    void testGetValueExpired() throws InterruptedException {
        db.put("exp_getValue", RedisValue.string("value"), 100);
        assertNotNull(db.getValue("exp_getValue"));
        
        Thread.sleep(150);
        assertNull(db.getValue("exp_getValue"));
    }

    @Test
    @DisplayName("getValue: Returns value before expiry")
    void testGetValueBeforeExpiry() {
        db.put("ttl_getValue", RedisValue.string("value"), 5000);
        RedisValue value = db.getValue("ttl_getValue");
        
        assertNotNull(value);
        assertEquals(RedisValue.Type.STRING, value.getType());
        assertEquals("value", value.asString());
    }

    // ==================== Tests for getTyped() ====================

    @Test
    @DisplayName("getTyped: Returns STRING data when type matches")
    void testGetTypedStringMatch() {
        db.put("typed_str", RedisValue.string("hello"));
        String result = db.getTyped("typed_str", RedisValue.Type.STRING);
        
        assertNotNull(result);
        assertEquals("hello", result);
    }

    @Test
    @DisplayName("getTyped: Returns LIST data when type matches")
    void testGetTypedListMatch() {
        List<String> list = Arrays.asList("a", "b", "c");
        db.put("typed_list", RedisValue.list(list));
        List<String> result = db.getTyped("typed_list", RedisValue.Type.LIST);
        
        assertNotNull(result);
        assertEquals(list, result);
    }

    @Test
    @DisplayName("getTyped: Returns SET data when type matches")
    void testGetTypedSetMatch() {
        Set<String> set = new HashSet<>(Arrays.asList("x", "y"));
        db.put("typed_set", RedisValue.set(set));
        Set<String> result = db.getTyped("typed_set", RedisValue.Type.SET);
        
        assertNotNull(result);
        assertEquals(set, result);
    }

    @Test
    @DisplayName("getTyped: Returns HASH data when type matches")
    void testGetTypedHashMatch() {
        Map<String, String> hash = new HashMap<>();
        hash.put("k1", "v1");
        db.put("typed_hash", RedisValue.hash(hash));
        Map<String, String> result = db.getTyped("typed_hash", RedisValue.Type.HASH);
        
        assertNotNull(result);
        assertEquals(hash, result);
    }

    @Test
    @DisplayName("getTyped: Returns null for non-existent key")
    void testGetTypedNonExistent() {
        String result = db.getTyped("nonexistent", RedisValue.Type.STRING);
        assertNull(result);
    }

    @Test
    @DisplayName("getTyped: Throws exception when type mismatches")
    void testGetTypedTypeMismatch() {
        db.put("typed_mismatch", RedisValue.string("value"));
        
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            db.getTyped("typed_mismatch", RedisValue.Type.LIST);
        });
        
        assertTrue(exception.getMessage().contains("WRONGTYPE"));
    }

    @Test
    @DisplayName("getTyped: Returns null for expired key")
    void testGetTypedExpired() throws InterruptedException {
        db.put("exp_typed", RedisValue.string("value"), 100);
        assertNotNull(db.getTyped("exp_typed", RedisValue.Type.STRING));
        
        Thread.sleep(150);
        String result = db.getTyped("exp_typed", RedisValue.Type.STRING);
        assertNull(result);
    }

    @Test
    @DisplayName("getTyped: Works correctly with different types stored under different keys")
    void testGetTypedMultipleTypes() {
        db.put("multi_str", RedisValue.string("text"));
        db.put("multi_list", RedisValue.list(Arrays.asList("item")));
        
        String strResult = db.getTyped("multi_str", RedisValue.Type.STRING);
        List<String> listResult = db.getTyped("multi_list", RedisValue.Type.LIST);
        
        assertEquals("text", strResult);
        assertEquals(Arrays.asList("item"), listResult);
        
        // Type mismatch should throw exception
        assertThrows(IllegalStateException.class, () -> {
            db.getTyped("multi_str", RedisValue.Type.LIST);
        });
        assertThrows(IllegalStateException.class, () -> {
            db.getTyped("multi_list", RedisValue.Type.STRING);
        });
    }

    // ==================== Tests for getType() ====================

    @Test
    @DisplayName("getType: Returns STRING type for string values")
    void testGetTypeString() {
        db.put("type_str", RedisValue.string("value"));
        RedisValue.Type type = db.getType("type_str");
        
        assertEquals(RedisValue.Type.STRING, type);
    }

    @Test
    @DisplayName("getType: Returns LIST type for list values")
    void testGetTypeList() {
        db.put("type_list", RedisValue.list(Arrays.asList("a", "b")));
        RedisValue.Type type = db.getType("type_list");
        
        assertEquals(RedisValue.Type.LIST, type);
    }

    @Test
    @DisplayName("getType: Returns SET type for set values")
    void testGetTypeSet() {
        db.put("type_set", RedisValue.set(new HashSet<>(Arrays.asList("x"))));
        RedisValue.Type type = db.getType("type_set");
        
        assertEquals(RedisValue.Type.SET, type);
    }

    @Test
    @DisplayName("getType: Returns HASH type for hash values")
    void testGetTypeHash() {
        Map<String, String> hash = new HashMap<>();
        hash.put("field", "value");
        db.put("type_hash", RedisValue.hash(hash));
        RedisValue.Type type = db.getType("type_hash");
        
        assertEquals(RedisValue.Type.HASH, type);
    }

    @Test
    @DisplayName("getType: Returns null for non-existent key")
    void testGetTypeNonExistent() {
        RedisValue.Type type = db.getType("nonexistent_type");
        assertNull(type);
    }

    @Test
    @DisplayName("getType: Returns null for expired key")
    void testGetTypeExpired() throws InterruptedException {
        db.put("exp_type", RedisValue.string("value"), 100);
        assertNotNull(db.getType("exp_type"));
        
        Thread.sleep(150);
        RedisValue.Type type = db.getType("exp_type");
        assertNull(type);
    }

    @Test
    @DisplayName("getType: Returns correct type before expiry")
    void testGetTypeBeforeExpiry() {
        db.put("ttl_type", RedisValue.string("value"), 5000);
        RedisValue.Type type = db.getType("ttl_type");
        
        assertNotNull(type);
        assertEquals(RedisValue.Type.STRING, type);
    }

    @Test
    @DisplayName("getType: Handles type changes on key overwrite")
    void testGetTypeOverwrite() {
        db.put("overwrite_type", RedisValue.string("initial"));
        assertEquals(RedisValue.Type.STRING, db.getType("overwrite_type"));
        
        db.put("overwrite_type", RedisValue.list(Arrays.asList("new")));
        assertEquals(RedisValue.Type.LIST, db.getType("overwrite_type"));
    }
}

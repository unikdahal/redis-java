package com.redis.storage;

/**
 * Exception thrown when attempting to access a Redis value with the wrong type.
 * This exception should be caught by command handlers and converted to a RESP error response.
 */
public class RedisWrongTypeException extends RuntimeException {
    
    /**
     * Creates a new RedisWrongTypeException with the specified error message.
     *
     * @param message the error message describing the type mismatch
     */
    public RedisWrongTypeException(String message) {
        super(message);
    }
}

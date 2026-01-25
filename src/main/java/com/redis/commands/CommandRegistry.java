package com.redis.commands;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for all available Redis commands.
 * Supports dynamic command registration and lookup.
 * This is a singleton initialized with built-in commands.
 *
 * Optimizations:
 * - Case-insensitive lookups with single toUpperCase() call
 * - ConcurrentHashMap with capacity hints for faster lookup
 */
public class CommandRegistry {
    private static CommandRegistry INSTANCE;

    private final Map<String, ICommand> registry = new ConcurrentHashMap<>(16);

    /**
     * Initializes the singleton CommandRegistry and registers the built-in commands.
     *
     * Registers the default command implementations: SetCommand, GetCommand, DelCommand, and RPushCommand.
     */
    private CommandRegistry() {
        // Register built-in commands
        register(new SetCommand());
        register(new GetCommand());
        register(new DelCommand());
        register(new RPushCommand());
        register(new LRangeCommand());
    }

    /**
     * Get the singleton instance of CommandRegistry.
     */
    public static CommandRegistry getInstance() {
        if (INSTANCE == null) {
            synchronized (CommandRegistry.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CommandRegistry();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Register a command in the registry.
     * Command names are stored in uppercase for case-insensitive lookup.
     */
    public void register(ICommand cmd) {
        String cmdName = cmd.name().toUpperCase();
        registry.put(cmdName, cmd);
    }

    /**
     * Look up a command by name (case-insensitive).
     * Returns the command instance or null if not found.
     * Optimization: Single toUpperCase() call, direct HashMap lookup
     */
    public ICommand get(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return registry.get(name.toUpperCase());
    }

    /**
     * Check if a command is registered.
     */
    public boolean exists(String name) {
        return get(name) != null;
    }

    /**
     * Get all registered command names (unmodifiable).
     */
    public Set<String> getRegisteredCommands() {
        return Collections.unmodifiableSet(registry.keySet());
    }

    /**
     * Get the number of registered commands.
     */
    public int size() {
        return registry.size();
    }
}
package com.redis.commands;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for all available Redis commands.
 * Supports dynamic command registration and lookup.
 * This is a singleton initialized with built-in commands.
 */
public class CommandRegistry {
    private static CommandRegistry INSTANCE;

    private final Map<String, ICommand> registry = new ConcurrentHashMap<>();

    private CommandRegistry() {
        // Register built-in commands
        register(new SetCommand());
        register(new GetCommand());
        register(new DelCommand());
        System.out.println("[CommandRegistry] Initialized with commands: " + registry.keySet());
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
        System.out.println("[CommandRegistry] Registered command: " + cmdName);
    }

    /**
     * Look up a command by name (case-insensitive).
     * Returns the command instance or null if not found.
     */
    public ICommand get(String name) {
        if (name == null) {
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
     * Get all registered command names.
     */
    public java.util.Set<String> getRegisteredCommands() {
        return new java.util.HashSet<>(registry.keySet());
    }
}

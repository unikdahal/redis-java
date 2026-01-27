package com.redis.commands;

import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Central Command Registry.
 * <p>
 * <b>Role:</b> Acts as the directory for all executable commands. It maps a string (e.g., "SET")
 * to the actual Java object capable of executing that logic.
 * <p>
 * <b>Design Pattern: Singleton</b>
 * We use a Singleton because the list of commands is static for the application's lifetime.
 * Re-scanning for commands for every client connection would be incredibly slow.
 * <p>
 * <b>Design Pattern: Strategy / Command</b>
 * This registry enables the "Command Pattern". The network layer doesn't need to know how "SET" works;
 * it just retrieves the command object and calls .execute().
 */
public class CommandRegistry {

    // The single instance of this class (Volatile is implied by the memory model of synchronized,
    // but usually good practice to mark volatile in double-checked locking to prevent instruction reordering).
    private static volatile CommandRegistry INSTANCE;

    // The actual storage.
    // Key = Command Name (UPPERCASE), Value = Command Object.
    // We use ConcurrentHashMap because the registry might be read by multiple Netty threads simultaneously.
    // While writes happen mostly at startup, safe reads are critical.
    private final Map<String, ICommand> registry = new ConcurrentHashMap<>(32);

    /**
     * Private constructor to enforce Singleton usage.
     * <p>
     * <b>Mechanism: ServiceLoader (SPI)</b>
     * Instead of hardcoding "new SetCommand()", we ask Java to look at the classpath.
     * Java looks for a file: META-INF/services/com.redis.commands.ICommand
     * It reads the class names listed there and instantiates them.
     */
    private CommandRegistry() {
        // Step 1: Initialize the loader for the ICommand interface
        ServiceLoader<ICommand> loader = ServiceLoader.load(ICommand.class);

        // Step 2: Iterate through found implementations.
        // The ServiceLoader lazily instantiates the classes as we iterate.
        for (ICommand cmd : loader) {
            register(cmd);
        }

        // Logging is helpful to verify that your META-INF file is set up correctly.
        System.out.println("[Redis] Registered " + registry.size() + " commands");
    }

    /**
     * The Holder Class.
     * <p>
     * 1. <b>Lazy:</b> This class is NOT loaded when CommandRegistry is loaded.
     * It is only loaded when getInstance() is called for the first time.
     * 2. <b>Thread-Safe:</b> The JVM guarantees that static field initialization
     * (INSTANCE = new ...) happens atomically. No synchronized keyword needed.
     */
    private static class RegistryHolder {
        private static final CommandRegistry INSTANCE = new CommandRegistry();
    }

    /**
     * Get the singleton instance.
     * Triggers the loading of RegistryHolder and the creation of INSTANCE.
     */
    public static CommandRegistry getInstance() {
        return RegistryHolder.INSTANCE;
    }

    /**
     * Registers a command into the map.
     * <p>
     * <b>Normalization:</b> We store all keys in UPPERCASE. This ensures that
     * "set", "Set", and "SET" all resolve to the same entry.
     */
    public void register(ICommand cmd) {
        String cmdName = cmd.name().toUpperCase();
        registry.put(cmdName, cmd);
    }

    /**
     * Retrieves the command object for a given name.
     * <p>
     * <b>Performance:</b> This is a "Hot Path" method called for every single request.
     * It must be O(1) and very fast.
     *
     * @param name The command name (e.g., "set")
     * @return The ICommand instance, or null if not found.
     */
    public ICommand get(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        // Convert input to uppercase to match the storage key format.
        return registry.get(name.toUpperCase());
    }

    /**
     * Utility to check command existence without retrieving it.
     */
    public boolean exists(String name) {
        return get(name) != null;
    }

    /**
     * Returns a Read-Only view of all available commands.
     * Useful for the "COMMAND" command in Redis which lists capabilities.
     */
    public Set<String> getRegisteredCommands() {
        return Collections.unmodifiableSet(registry.keySet());
    }

    public int size() {
        return registry.size();
    }
}
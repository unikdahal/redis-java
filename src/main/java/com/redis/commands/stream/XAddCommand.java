package com.redis.commands.stream;

import com.redis.commands.ICommand;
import com.redis.storage.RedisDatabase;
import com.redis.storage.RedisValue;
import com.redis.util.StreamId;
import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of the XADD command.
 * <p>
 * <b>Syntax:</b> XADD key ID field value [field value ...]
 * <p>
 * <b>Role:</b> Appends a new entry to a stream. This is the "Writer" command for streams.
 * <p>
 * <b>Concurrency Strategy:</b>
 * This command relies heavily on {@link RedisDatabase#compute} to ensure atomic ID generation.
 * Since Stream IDs must be strictly monotonic (always increasing), we must lock the stream
 * while we calculate the next ID to prevent two clients from generating the same ID simultaneously.
 */
public class XAddCommand implements ICommand {

    // Standard Redis Error Messages
    private static final String ERR_WRONG_ARGS = "-ERR wrong number of arguments for 'XADD' command\r\n";
    private static final String ERR_WRONG_TYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private static final String ERR_ID_TOO_SMALL = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
    private static final String ERR_ID_ZERO = "-ERR The ID specified in XADD must be greater than 0-0\r\n";

    @Override
    public String execute(List<String> args, ChannelHandlerContext ctx) {
        // Minimum args: key, ID, field, value (4 args -> size 4).
        // Note: The List<String> args usually includes key at 0.
        // Format: XADD <key> <id> <field> <value> ...
        if (args.size() < 3) {
            return ERR_WRONG_ARGS;
        }

        String key = args.get(0);
        String idArg = args.get(1);

        // Validation: Fields and Values must come in pairs
        int fieldStart = 2;
        if ((args.size() - fieldStart) % 2 != 0) {
            return ERR_WRONG_ARGS;
        }

        // Parse fields into a LinkedHashMap to preserve insertion order (Redis convention)
        Map<String, String> fields = new LinkedHashMap<>();
        for (int i = fieldStart; i < args.size(); i += 2) {
            fields.put(args.get(i), args.get(i + 1));
        }

        RedisDatabase db = RedisDatabase.getInstance();

        // We use AtomicReferences to extract results/errors from inside the lambda
        AtomicReference<String> error = new AtomicReference<>(null);
        AtomicReference<StreamId> addedId = new AtomicReference<>(null);

        // CRITICAL SECTION: Atomic Read-Modify-Write
        // We lock the key to ensure no one else inserts while we determine the next ID.
        db.compute(key, existing -> {
            ConcurrentSkipListMap<StreamId, Map<String, String>> streamMap;

            // 1. Initialization / Type Check
            if (existing == null) {
                // New Key: Create a new SkipList (Ordered Thread-Safe Map)
                streamMap = new ConcurrentSkipListMap<>();
            } else if (existing.getType() != RedisValue.Type.STREAM) {
                // Wrong Type: Cannot append stream data to a String/List
                error.set(ERR_WRONG_TYPE);
                return existing;
            } else {
                // Existing Key: Cast the raw data
                @SuppressWarnings("unchecked")
                var data = (Map<StreamId, Map<String, String>>) existing.getData();
                // We know it's a SkipList because we created it that way in RedisValue factory
                streamMap = (ConcurrentSkipListMap<StreamId, Map<String, String>>) data;
            }

            // 2. Determine Context (What is the last ID?)
            // If stream is empty, assume "0-0" is the predecessor
            StreamId lastId = streamMap.isEmpty() ? new StreamId(0, 0) : streamMap.lastKey();
            StreamId newId;

            try {
                // 3. ID Generation Logic
                if (idArg.equals("*")) {
                    // AUTO-GENERATE: "*"
                    long now = System.currentTimeMillis();
                    if (now > lastId.time()) {
                        // Standard case: New millisecond, reset sequence to 0
                        newId = new StreamId(now, 0);
                    } else {
                        // Collision or Clock Skew: Time is the same (or older) than the last entry.
                        // We must increment the sequence number of the LAST entry's time.
                        // This handles high-throughput bursts within the same ms.
                        newId = new StreamId(lastId.time(), lastId.sequence() + 1);
                    }
                } else if (idArg.endsWith("-*")) {
                    // PARTIAL ID: "123456-*"
                    long ms = Long.parseLong(idArg.substring(0, idArg.length() - 2));

                    if (ms < lastId.time()) {
                        error.set(ERR_ID_TOO_SMALL);
                        return existing;
                    }
                    // If time is the same, increment sequence. If time is new, seq is 0.
                    long seq = (ms == lastId.time()) ? lastId.sequence() + 1 : 0;
                    newId = new StreamId(ms, seq);
                } else {
                    // EXPLICIT ID: "123456-0"
                    newId = StreamId.parse(idArg);
                }

                // 4. Validation Rules
                // Rule A: ID must be > 0-0
                if (newId.time() == 0 && newId.sequence() == 0) {
                    error.set(ERR_ID_ZERO);
                    return existing;
                }

                // Rule B: ID must be strictly greater than the last ID
                // Note: If stream is empty, lastId is 0-0, so any valid ID passes.
                if (!streamMap.isEmpty() && !newId.isGreaterThan(lastId)) {
                    error.set(ERR_ID_TOO_SMALL);
                    return existing;
                }

                // 5. Execution: Insert into map
                streamMap.put(newId, fields);
                addedId.set(newId);

                // Return wrapped value to update DB (or keep existing reference)
                return RedisValue.stream(streamMap);

            } catch (Exception e) {
                // Handle parsing errors (e.g., malformed ID string)
                error.set("-ERR " + e.getMessage() + "\r\n");
                return existing;
            }
        });

        // 6. Response Handling
        // If the lambda set an error, return it.
        if (error.get() != null) {
            return error.get();
        }

        // Otherwise return the ID we just generated/inserted.
        String idStr = addedId.get().toString();
        // Redis Bulk String format: $<len>\r\n<data>\r\n
        return "$" + idStr.length() + "\r\n" + idStr + "\r\n";
    }

    @Override
    public String name() {
        return "XADD";
    }
}
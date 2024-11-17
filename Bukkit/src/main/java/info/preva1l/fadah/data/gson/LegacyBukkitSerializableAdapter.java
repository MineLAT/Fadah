package info.preva1l.fadah.data.gson;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import info.preva1l.fadah.Fadah;
import org.bukkit.configuration.serialization.ConfigurationSerializable;
import org.bukkit.configuration.serialization.ConfigurationSerialization;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

public class LegacyBukkitSerializableAdapter implements JsonSerializer<ConfigurationSerializable>, JsonDeserializer<ConfigurationSerializable> {
    private static final Type OBJECT_STRING_MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    @Deprecated
    @Override
    public ConfigurationSerializable deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        try {
            final Map<String, Object> map = new LinkedHashMap<>();

            for (Map.Entry<String, JsonElement> entry : json.getAsJsonObject().entrySet()) {
                final JsonElement value = entry.getValue();
                final String name = entry.getKey();

                if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber() && (value.getAsNumber() instanceof Integer || value.getAsDouble() % 1 == 0)) {
                    map.put(name, value.getAsInt());
                } else if (value.isJsonObject() && value.getAsJsonObject().has(ConfigurationSerialization.SERIALIZED_TYPE_KEY)) {
                    map.put(name, this.deserialize(value, value.getClass(), context));
                } else {
                    map.put(name, context.deserialize(value, Object.class));
                }
            }

            return ConfigurationSerialization.deserializeObject(map);
        } catch (Exception e) {
            Fadah.getConsole().log(Level.WARNING, e.getMessage(), e);
            return null;
        }
    }

    @Deprecated(forRemoval = true)
    @Override
    public JsonElement serialize(ConfigurationSerializable src, Type typeOfSrc, JsonSerializationContext context) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put(ConfigurationSerialization.SERIALIZED_TYPE_KEY, ConfigurationSerialization.getAlias(src.getClass()));
        map.putAll(src.serialize());
        return context.serialize(map, OBJECT_STRING_MAP_TYPE);
    }
}
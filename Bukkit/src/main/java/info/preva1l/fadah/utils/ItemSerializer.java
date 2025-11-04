package info.preva1l.fadah.utils;

import lombok.experimental.UtilityClass;
import org.bukkit.Bukkit;
import org.bukkit.inventory.ItemStack;
import org.bukkit.util.io.BukkitObjectInputStream;
import org.bukkit.util.io.BukkitObjectOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamConstants;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Map;

@UtilityClass
public class ItemSerializer {

    public static final int DATA_VERSION;
    public static final String INVALID_VERSION_MESSAGE = "Newer version! Server downgrades are not supported!";
    public static final IllegalArgumentException INVALID_VERSION_EXCEPTION = new IllegalArgumentException(INVALID_VERSION_MESSAGE);

    static {
        final String serverPackage = Bukkit.getServer().getClass().getPackage().getName();
        try {
            final Class<?> magicNumbersClass = Class.forName(serverPackage + ".util.CraftMagicNumbers");
            final Object craftMagicNumbers = magicNumbersClass.getDeclaredField("INSTANCE").get(null);
            DATA_VERSION = (int) magicNumbersClass.getDeclaredMethod("getDataVersion").invoke(craftMagicNumbers);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @NotNull
    public static String serialize(@NotNull ItemStack item) {
        return Base64.getEncoder().encodeToString(item.serializeAsBytes());
    }

    @NotNull
    public static ItemStack deserialize(@NotNull String source) {
        final byte[] data = Base64.getDecoder().decode(source.replaceAll("\\s", ""));
        if (((data[0] << 8) | (data[1] & 0xFF)) == ObjectStreamConstants.STREAM_MAGIC) { // Old format
            return bukkitDeserialize(data);
        } else {
            try {
                return ItemStack.deserializeBytes(data);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals(INVALID_VERSION_MESSAGE)) {
                    throw INVALID_VERSION_EXCEPTION;
                } else {
                    throw e;
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    @NotNull
    public static String bukkitSerialize(@NotNull ItemStack... items) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(); BukkitObjectOutputStream dataOutput = new BukkitObjectOutputStream(outputStream)) {

            dataOutput.writeInt(items.length);

            for (ItemStack item : items)
                dataOutput.writeObject(item);

            return Base64.getEncoder().encodeToString(outputStream.toByteArray());

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @NotNull
    public static ItemStack bukkitDeserialize(@NotNull String source) {
        return bukkitDeserialize(Base64.getDecoder().decode(source.replaceAll("\\s", "")));
    }

    @SuppressWarnings("deprecation")
    @NotNull
    public static ItemStack bukkitDeserialize(byte[] data) {
        final Integer version = bukkitVersion(data);
        if (version != null && version > DATA_VERSION) {
            throw INVALID_VERSION_EXCEPTION;
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data); BukkitObjectInputStream dataInput = new BukkitObjectInputStream(inputStream)) {

            ItemStack[] items = new ItemStack[dataInput.readInt()];

            for (int i = 0; i < items.length; i++)
                items[i] = (ItemStack) dataInput.readObject();

            return items[0];
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Nullable
    public static Integer bukkitVersion(byte[] data) {
        try (WrapperInputStream in = new WrapperInputStream(new ByteArrayInputStream(data))) {
            final Object object = in.readObject();
            if (object instanceof Map<?, ?> map) {
                final Object version = map.get("v");
                if (version instanceof Number number)
                    return number.intValue();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return null;
    }

    private static class WrapperInputStream extends BukkitObjectInputStream {

        private static final Class<?> WRAPPER_TYPE;
        private static final MethodHandle WRAPPER_MAP;

        static {
            try {
                WRAPPER_TYPE = Class.forName("org.bukkit.util.io.Wrapper");

                final MethodHandles.Lookup lookup = MethodHandles.lookup();
                final Field field = WRAPPER_TYPE.getDeclaredField("map");
                field.setAccessible(true);
                WRAPPER_MAP = lookup.unreflectGetter(field);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }

        public WrapperInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Object resolveObject(Object obj) throws IOException {
            if (WRAPPER_TYPE.isInstance(obj)) {
                try {
                    return WRAPPER_MAP.invoke(obj);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }

            return super.resolveObject(obj);
        }
    }
}

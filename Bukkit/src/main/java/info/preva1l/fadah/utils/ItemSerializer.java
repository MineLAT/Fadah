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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectStreamConstants;
import java.util.Base64;
import java.util.zip.GZIPInputStream;

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
        if (((data[1] << 8) | data[0]) == GZIPInputStream.GZIP_MAGIC) {
            try {
                return ItemStack.deserializeBytes(data);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals(INVALID_VERSION_MESSAGE)) {
                    throw INVALID_VERSION_EXCEPTION;
                } else {
                    throw e;
                }
            }
        } else {
            return bukkitDeserialize(data);
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
        if (version == null || version > DATA_VERSION) {
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
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {

            // Skip header
            final short magic = in.readShort();
            if (magic != ObjectStreamConstants.STREAM_MAGIC) {
                throw new IOException("Not a valid object stream");
            }
            // ObjectStreamConstants.STREAM_VERSION
            final short version = in.readShort();

            while (in.available() > 0) {
                if (in.readByte() == ObjectStreamConstants.TC_STRING) {
                    if ("v".equals(in.readUTF())) {
                        return in.readInt();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}

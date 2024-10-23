package info.preva1l.fadah.data.fixers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import info.preva1l.fadah.data.dao.SqlDao;
import info.preva1l.fadah.data.gson.ConfigurationSerializableAdapter;
import info.preva1l.fadah.data.handler.DataHandler;
import info.preva1l.fadah.data.handler.HikariHandler;
import info.preva1l.fadah.records.CollectableItem;
import info.preva1l.fadah.records.CollectionBox;
import info.preva1l.fadah.records.ExpiredItems;
import info.preva1l.fadah.records.Listing;
import info.preva1l.fadah.utils.ItemSerializer;
import lombok.RequiredArgsConstructor;
import org.bukkit.configuration.serialization.ConfigurationSerializable;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
public class HikariFixer {

    private static final Gson GSON = new GsonBuilder()
            .registerTypeHierarchyAdapter(ConfigurationSerializable.class, new ConfigurationSerializableAdapter())
            .serializeNulls().disableHtmlEscaping().create();
    private static final Type COLLECTABLE_ITEM_TYPE = new TypeToken<ArrayList<CollectableItem>>() {}.getType();

    private final HikariHandler handler;

    public void listings(@NotNull Connection con) throws SQLException {
        try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM `listings`"); PreparedStatement insert = con.prepareStatement(handler.getDao(Listing.class).sql(SqlDao.Statement.INSERT))) {
            final ResultSet result = stmt.executeQuery();

            while (result.next()) {
                insert.setString(1, result.getString("uuid"));
                insert.setString(2, result.getString("ownerUUID"));
                insert.setString(3, result.getString("ownerName"));
                insert.setString(4, result.getString("itemStack"));
                insert.setString(5, result.getString("category"));
                insert.setDouble(6, result.getDouble("price"));
                insert.setDouble(7, result.getDouble("tax"));
                insert.setLong(8, result.getLong("creationDate"));
                insert.setBoolean(9, result.getBoolean("biddable"));
                insert.setString(10, "");
                insert.setLong(11, Instant.now().toEpochMilli());

                insert.addBatch();
            }

            insert.executeBatch();
        }
    }

    public void collectionBox(@NotNull Connection con) throws SQLException {
        final long time = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
        try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM `collection_box`"); PreparedStatement insert = con.prepareStatement(handler.getDao(CollectionBox.class).sql(SqlDao.Statement.INSERT))) {
            final ResultSet result = stmt.executeQuery();

            while (result.next()) {
                insert.setString(1, UUID.randomUUID().toString());
                insert.setString(2, DataHandler.DUMMY_ID.toString());
                insert.setString(3, result.getString("playerUUID"));
                insert.setString(4, result.getString("itemStack"));
                insert.setLong(5, time);
                insert.setLong(6, result.getLong("dateAdded"));
                insert.setBoolean(7, false);

                insert.addBatch();
            }

            insert.executeBatch();
        }
    }

    public void collectionBoxV2(@NotNull Connection con) throws SQLException {
        final long time = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
        try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM `collection_boxV2`"); PreparedStatement insert = con.prepareStatement(handler.getDao(CollectionBox.class).sql(SqlDao.Statement.INSERT))) {
            final ResultSet result = stmt.executeQuery();

            while (result.next()) {
                final String buyerId = result.getString("playerUUID");
                final List<CollectableItem> items = GSON.fromJson(result.getString("items"), COLLECTABLE_ITEM_TYPE);
                for (CollectableItem item : items) {
                    insert.setString(1, item.id().toString());
                    insert.setString(2, item.owner().toString());
                    insert.setString(3, buyerId);
                    insert.setString(4, ItemSerializer.serialize(item.itemStack()));
                    insert.setLong(5, time);
                    insert.setLong(6, item.dateAdded());
                    insert.setBoolean(7, false);

                    insert.addBatch();
                }
            }

            insert.executeBatch();
        }
    }

    public void expiredItems(@NotNull Connection con) throws SQLException {
        final long time = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
        try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM `expired_items`"); PreparedStatement insert = con.prepareStatement(handler.getDao(ExpiredItems.class).sql(SqlDao.Statement.INSERT))) {
            final ResultSet result = stmt.executeQuery();

            while (result.next()) {
                insert.setString(1, UUID.randomUUID().toString());
                insert.setString(2, result.getString("playerUUID"));
                insert.setString(3, result.getString("itemStack"));
                insert.setLong(4, time);
                insert.setLong(5, result.getLong("dateAdded"));
                insert.setBoolean(6, false);

                insert.addBatch();
            }

            insert.executeBatch();
        }
    }

    public void expiredItemsV2(@NotNull Connection con) throws SQLException {
        final long time = Instant.now().minus(2, ChronoUnit.DAYS).toEpochMilli();
        try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM `expired_itemsV2`"); PreparedStatement insert = con.prepareStatement(handler.getDao(ExpiredItems.class).sql(SqlDao.Statement.INSERT))) {
            final ResultSet result = stmt.executeQuery();

            while (result.next()) {
                final String ownerId = result.getString("playerUUID");
                final List<CollectableItem> items = GSON.fromJson(result.getString("items"), COLLECTABLE_ITEM_TYPE);
                for (CollectableItem item : items) {
                    insert.setString(1, item.id().toString());
                    insert.setString(2, ownerId);
                    insert.setString(3, ItemSerializer.serialize(item.itemStack()));
                    insert.setLong(4, time);
                    insert.setLong(5, item.dateAdded());
                    insert.setBoolean(6, false);

                    insert.addBatch();
                }
            }

            insert.executeBatch();
        }
    }
}

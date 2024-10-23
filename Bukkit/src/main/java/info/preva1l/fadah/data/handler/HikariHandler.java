package info.preva1l.fadah.data.handler;

import com.google.common.base.Enums;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.config.Config;
import info.preva1l.fadah.data.DatabaseType;
import info.preva1l.fadah.data.dao.SqlDao;
import info.preva1l.fadah.data.dao.hikari.CollectionBoxHikariDao;
import info.preva1l.fadah.data.dao.hikari.ExpiredItemsHikariDao;
import info.preva1l.fadah.data.dao.hikari.HistoryHikariDao;
import info.preva1l.fadah.data.dao.hikari.ListingHikariDao;
import info.preva1l.fadah.data.fixers.HikariFixer;
import info.preva1l.fadah.records.CollectionBox;
import info.preva1l.fadah.records.ExpiredItems;
import info.preva1l.fadah.records.History;
import info.preva1l.fadah.records.Listing;
import lombok.Getter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class HikariHandler implements DatabaseHandler {

    private static final String DATABASE_FILE_NAME = "FadahData.db";
    private static final String SQL_CULL = "DELETE FROM `items` WHERE `update` < ? AND `collected` = ?;";

    private final Config.Database conf = Config.i().getDatabase();
    private final Map<Class<?>, SqlDao<?>> daos = new HashMap<>();
    private final String driverClass;

    @Getter
    private boolean connected = false;
    private HikariDataSource hikari;
    private HikariFixer fixer;

    public HikariHandler() {
        this.driverClass = switch (getType()) {
            case MONGO -> throw new IllegalArgumentException();
            case MYSQL -> "com.mysql.cj.jdbc.Driver";
            case MARIADB -> "org.mariadb.jdbc.Driver";
            case POSTGRESQL -> "org.postgresql.Driver";
            case SQLITE -> "org.sqlite.JDBC";
            case H2 -> "org.h2.Driver";
        };
    }

    @Override
    public void connect() {
        final HikariConfig config = new HikariConfig();
        config.setPoolName("FadahHikariPool");
        config.setDriverClassName(this.driverClass);

        if (getType().isLocal()) {
            final File file = new File(Fadah.getINSTANCE().getDataFolder(), DATABASE_FILE_NAME);
            try {
                if (file.createNewFile()) {
                    Fadah.getConsole().info("Created the " + getType().name() + " database file");
                }
            } catch (IOException e) {
                Fadah.getConsole().log(Level.SEVERE, "Cannot create database file", e);
            }

            config.setJdbcUrl(String.format("jdbc:%s:%s", getType().getId(), file.getAbsolutePath()));
            config.setConnectionTestQuery("SELECT 1");
            config.setMaxLifetime(60000);
            config.setIdleTimeout(45000);
            config.setMaximumPoolSize(50);

            this.hikari = new HikariDataSource(config);

            backup(file);
        } else {
            config.setAutoCommit(true);
            config.setJdbcUrl(conf.getUri());
            if (!conf.getUri().contains("@")) {
                config.setUsername(conf.getUsername());
                config.setPassword(conf.getPassword());
            }

            config.setMaximumPoolSize(conf.getAdvanced().getPoolSize());
            config.setMinimumIdle(conf.getAdvanced().getMinIdle());
            config.setMaxLifetime(conf.getAdvanced().getMaxLifetime());
            config.setKeepaliveTime(conf.getAdvanced().getKeepaliveTime());
            config.setConnectionTimeout(conf.getAdvanced().getConnectionTimeout());

            final Properties properties = new Properties();
            properties.put("cachePrepStmts", "true");
            properties.put("prepStmtCacheSize", "250");
            properties.put("prepStmtCacheSqlLimit", "2048");
            properties.put("useServerPrepStmts", "true");
            properties.put("useLocalSessionState", "true");
            properties.put("useLocalTransactionState", "true");
            properties.put("rewriteBatchedStatements", "true");
            properties.put("cacheResultSetMetadata", "true");
            properties.put("cacheServerConfiguration", "true");
            properties.put("elideSetAutoCommits", "true");
            properties.put("maintainTimeStats", "false");

            config.setDataSourceProperties(properties);

            this.hikari = new HikariDataSource(config);
        }

        registerDaos();

        this.fixer = new HikariFixer(this);

        // Execute schema
        connect(con -> {
            final Set<String> tables = getTables(con);
            if (tables.contains("items")) {
                return;
            }

            final String[] schema = getSchema();
            try (Statement stmt = con.createStatement()) {
                for (String sql : schema) {
                    stmt.addBatch(sql);
                }

                stmt.executeBatch();
            }

            if (tables.contains("listings")) {
                this.fixer.listings(con);
            }
            if (tables.contains("collection_box")) {
                this.fixer.collectionBox(con);
            }
            if (tables.contains("collection_boxV2")) {
                this.fixer.collectionBoxV2(con);
            }
            if (tables.contains("expired_items")) {
                this.fixer.expiredItems(con);
            }
            if (tables.contains("expired_itemsV2")) {
                this.fixer.expiredItemsV2(con);
            }
        }, "Failed to create database table.");

        // Cull old data
        final String cullData = conf.getAdvanced().getCullData().trim();
        if (!cullData.equals("0")) {
            final String[] split = cullData.split(" ", 2);
            final TimeUnit unit = Enums.getIfPresent(TimeUnit.class, split.length > 1 ? split[1] : "DAYS").or(TimeUnit.DAYS);
            try {
                final long time = Long.parseLong(split[0]);
                final long update = System.currentTimeMillis() - unit.toMillis(time);
                connect(con -> {
                    try (PreparedStatement stmt = con.prepareStatement(SQL_CULL)) {
                        stmt.setLong(1, update);
                        stmt.setBoolean(2, true);

                        stmt.execute();
                    }
                });
            } catch (NumberFormatException e) {
                Fadah.getConsole().log(Level.WARNING, "The time '" + cullData + "' is not a valid number", e);
            }
        }

        connected = true;
    }

    @Override
    public void destroy() {
        if (hikari != null) {
            hikari.close();
        }
    }

    @Override
    public void registerDaos() {
        daos.put(CollectionBox.class, new CollectionBoxHikariDao(this));
        daos.put(ExpiredItems.class, new ExpiredItemsHikariDao(this));
        daos.put(History.class, new HistoryHikariDao(this));
        daos.put(Listing.class, new ListingHikariDao(this));
    }

    @Override
    public void wipeDatabase() {
        // nothing yet
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getAll(Class<T> clazz) {
        return (List<T>) getDao(clazz).getAll();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> get(Class<T> clazz, UUID id) {
        return (Optional<T>) getDao(clazz).get(id);
    }

    @Override
    public <T> void save(Class<T> clazz, T t) {
        getDao(clazz).save(t);
    }

    @Override
    public <T> void update(Class<T> clazz, T t, String[] params) {
        getDao(clazz).update(t, params);
    }

    @Override
    public <T> void delete(Class<T> clazz, T t) {
        getDao(clazz).delete(t);
    }

    @Override
    public <T> void deleteSpecific(Class<T> clazz, T t, Object o) {
        getDao(clazz).deleteSpecific(t, o);
    }

    @NotNull
    public DatabaseType getType() {
        return conf.getType();
    }

    private Connection getConnection() throws SQLException {
        return hikari.getConnection();
    }

    @NotNull
    private String[] getSchema() {
        try {
            final InputStream input = Fadah.getINSTANCE().getResource(String.format("database/%s_schema.sql", getType().getId()));
            Objects.requireNonNull(input);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8).split(";");
        }  catch (IOException e) {
            throw new RuntimeException("Cannot get database schema for " + getType().name() + " database");
        }
    }

    /**
     * Gets the DAO for a specific class.
     *
     * @param clazz The class to get the DAO for.
     * @param <T>   The type of the class.
     * @return The DAO for the specified class.
     */
    @SuppressWarnings("unchecked")
    public <T> SqlDao<T> getDao(@NotNull Class<?> clazz) {
        if (!daos.containsKey(clazz))
            throw new IllegalArgumentException("No DAO registered for class " + clazz.getName());
        return (SqlDao<T>) daos.get(clazz);
    }

    public void connect(@NotNull SqlConsumer consumer) {
        connect(consumer, "Cannot execute database connection");
    }

    public void connect(@NotNull SqlConsumer consumer, @NotNull String msg) {
        try (Connection connection = hikari.getConnection()) {
            consumer.accept(connection);
        } catch (SQLException e) {
            Fadah.getConsole().log(Level.WARNING, msg, e);
        }
    }

    @Nullable
    @Contract("_, !null -> !null")
    public <T> T connect(@NotNull SqlFunction<T> function, @Nullable T def) {
        return connect(function, "Cannot execute database connection", def);
    }

    @Contract("_, _, !null -> !null")
    public <T> T connect(@NotNull SqlFunction<T> function, @NotNull String msg, @Nullable T def) {
        try (Connection connection = hikari.getConnection()) {
            return function.apply(connection);
        } catch (SQLException e) {
            Fadah.getConsole().log(Level.WARNING, msg, e);
        }
        return def;
    }

    private static void backup(@NotNull File file) {
        if (!file.exists()) {
            return;
        }

        final File backup = new File(file.getParent(), String.format("%s.bak", file.getName()));
        try {
            if (!backup.exists() || backup.delete()) {
                Files.copy(file.toPath(), backup.toPath());
            }
        } catch (IOException e) {
            Fadah.getConsole().log(Level.WARNING, "Failed to backup flat file database", e);
        }
    }

    @NotNull
    private static Set<String> getTables(@NotNull Connection con) throws SQLException {
        final Set<String> tables = new HashSet<>();
        try (ResultSet set = con.getMetaData().getTables(con.getCatalog(), null, "%", null)) {
            while (set.next()) {
                tables.add(set.getString(3));
            }
        }
        return tables;
    }

    @FunctionalInterface
    public interface SqlConsumer {
        void accept(@NotNull Connection connection) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlFunction<T> {
        T apply(@NotNull Connection connection) throws SQLException;
    }
}

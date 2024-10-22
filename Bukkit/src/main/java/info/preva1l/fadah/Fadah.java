package info.preva1l.fadah;

import info.preva1l.fadah.api.AuctionHouseAPI;
import info.preva1l.fadah.api.BukkitAuctionHouseAPI;
import info.preva1l.fadah.api.ListingEndEvent;
import info.preva1l.fadah.api.ListingEndReason;
import info.preva1l.fadah.cache.*;
import info.preva1l.fadah.commands.AuctionHouseCommand;
import info.preva1l.fadah.commands.MigrateCommand;
import info.preva1l.fadah.config.Config;
import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.config.Menus;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.data.DatabaseType;
import info.preva1l.fadah.hooks.HookManager;
import info.preva1l.fadah.hooks.impl.DiscordHook;
import info.preva1l.fadah.hooks.impl.EcoItemsHook;
import info.preva1l.fadah.hooks.impl.InfluxDBHook;
import info.preva1l.fadah.listeners.PlayerListener;
import info.preva1l.fadah.migrator.AuctionHouseMigrator;
import info.preva1l.fadah.migrator.MigratorManager;
import info.preva1l.fadah.migrator.zAuctionHouseMigrator;
import info.preva1l.fadah.multiserver.Broker;
import info.preva1l.fadah.multiserver.RedisBroker;
import info.preva1l.fadah.records.*;
import info.preva1l.fadah.utils.Metrics;
import info.preva1l.fadah.utils.StringUtils;
import info.preva1l.fadah.utils.TaskManager;
import info.preva1l.fadah.utils.commands.CommandManager;
import info.preva1l.fadah.utils.config.BasicConfig;
import info.preva1l.fadah.utils.guis.FastInvManager;
import info.preva1l.fadah.utils.guis.LayoutManager;
import info.preva1l.fadah.utils.logging.TransactionLogFormatter;
import info.preva1l.fadah.utils.logging.TransactionLogger;
import lombok.Getter;
import lombok.Setter;
import net.kyori.adventure.platform.bukkit.BukkitAudiences;
import net.milkbowl.vault.economy.Economy;
import net.william278.desertwell.util.UpdateChecker;
import net.william278.desertwell.util.Version;
import org.bukkit.Bukkit;
import org.bukkit.NamespacedKey;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.stream.Stream;

public final class Fadah extends JavaPlugin {
    private static final int METRICS_ID = 21651;
    private static final int SPIGOT_ID = 116157;
    private Version pluginVersion;

    @Getter private static Fadah INSTANCE;
    @Getter @Setter private static NamespacedKey customItemKey;

    @Getter private static Logger console;
    @Getter private final Logger transactionLogger = Logger.getLogger("AuctionHouse-Transactions");

    @Getter private BasicConfig categoriesFile;
    @Getter private BasicConfig menusFile;

    @Getter private Broker broker;
    @Getter private CommandManager commandManager;
    @Getter private Economy economy;
    @Getter private HookManager hookManager;
    @Getter private LayoutManager layoutManager;

    @Getter private BukkitAudiences adventureAudience;
    @Getter private MigratorManager migratorManager;

    private Metrics metrics;

    @Override
    public void onEnable() {
        INSTANCE = this;
        pluginVersion = Version.fromString(getDescription().getVersion());
        console = getLogger();
        hookManager = new HookManager();
        adventureAudience = BukkitAudiences.create(this);

        if (!hookIntoVault()) {
            getConsole().severe("------------------------------------------");
            getConsole().severe("Disabled due to no Vault dependency found!");
            getConsole().severe("------------------------------------------");
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        } else {
            getConsole().info("Vault Hooked!");
        }

        loadMenus();
        loadFiles();
        loadDataAndPopulateCaches();
        loadCommands();

        getServer().getPluginManager().registerEvents(new PlayerListener(), this);
        TaskManager.Async.runTask(this, listingExpiryTask(), 10L);
        FastInvManager.register(this);

        loadBroker();

        customItemKey = NamespacedKey.minecraft("auctionhouse");

        loadHooks();
        loadMigrators();

        initLogger();
        setupMetrics();

        getConsole().info("Enabling the API...");
        AuctionHouseAPI.setInstance(new BukkitAuctionHouseAPI());
        getConsole().info("API Enabled!");

        Bukkit.getConsoleSender().sendMessage(StringUtils.colorize("&2&l------------------------------"));
        Bukkit.getConsoleSender().sendMessage(StringUtils.colorize("&a Finally a Decent Auction House"));
        Bukkit.getConsoleSender().sendMessage(StringUtils.colorize("&a   has successfully started!"));
        Bukkit.getConsoleSender().sendMessage(StringUtils.colorize("&2&l------------------------------"));

        TaskManager.Sync.runLater(this, this::checkForUpdates, 60L);
    }

    @Override
    public void onDisable() {
        DatabaseManager.getInstance().shutdown();
        if (broker != null) broker.destroy();
        if (metrics != null) metrics.shutdown();
        Optional<InfluxDBHook> hook = Fadah.getINSTANCE().getHookManager().getHook(InfluxDBHook.class);
        if (Config.i().getHooks().getInfluxdb().isEnabled() && hook.isPresent() && hook.get().isEnabled()) {
            hook.get().destroy();
        }
    }

    private Runnable listingExpiryTask() {
        return () -> {
            for (UUID key : ListingCache.getListings().keySet()) {
                Listing listing = ListingCache.getListing(key);
                if (Instant.now().toEpochMilli() >= listing.getDeletionDate()) {
                    ListingCache.removeListing(listing);
                    if (Config.i().getDatabase().getType() == DatabaseType.MONGO) {
                        DatabaseManager.getInstance().delete(Listing.class, listing);
                    }

                    CollectableItem item = new CollectableItem(listing.getId(), listing.getOwner(), listing.getItemStack(), Instant.now().toEpochMilli());
                    ExpiredListingsCache.addItem(listing.getOwner(), item);
                    DatabaseManager.getInstance().save(ExpiredItems.class, ExpiredItems.of(listing.getOwner()));

                    TransactionLogger.listingExpired(listing);

                    getServer().getPluginManager().callEvent(new ListingEndEvent(listing, ListingEndReason.EXPIRED));
                }
            }
        };
    }

    private void loadCommands() {
        getConsole().info("Loading commands...");
        this.commandManager = new CommandManager(this);
        new AuctionHouseCommand(this);
        new MigrateCommand(this);
        getConsole().info("Commands Loaded!");
    }

    private void loadFiles() {
        getConsole().info("Loading Configuration Files...");
        categoriesFile = new BasicConfig(this, "categories.yml");

        Config.i();
        Lang.i();

        categoriesFile.save();
        categoriesFile.load();
        getConsole().info("Configuration Files Loaded!");
    }

    private void loadMenus() {
        layoutManager = new LayoutManager();

        menusFile = new BasicConfig(this, "menus/misc.yml");
        Menus.loadDefault();

        Stream.of(
                new BasicConfig(this, "menus/main.yml"),
                new BasicConfig(this, "menus/new-listing.yml"),
                new BasicConfig(this, "menus/expired-listings.yml"),
                new BasicConfig(this, "menus/active-listings.yml"),
                new BasicConfig(this, "menus/historic-items.yml"),
                new BasicConfig(this, "menus/confirm.yml"),
                new BasicConfig(this, "menus/collection-box.yml"),
                new BasicConfig(this, "menus/profile.yml"),
                new BasicConfig(this, "menus/view-listings.yml")
        ).forEach(layoutManager::loadLayout);
    }

    private boolean hookIntoVault() {
        getConsole().info("Hooking into Vault...");
        if (INSTANCE.getServer().getPluginManager().getPlugin("Vault") == null) {
            getConsole().severe("Vault not installed");
            return false;
        }

        RegisteredServiceProvider<Economy> rsp = Bukkit.getServer().getServicesManager().getRegistration(Economy.class);
        if (rsp == null) {
            getConsole().severe("No Economy Plugin Installed");
            return false;
        }
        economy = rsp.getProvider();

        return true;
    }


    private void loadDataAndPopulateCaches() {
        DatabaseManager.getInstance(); // Make the connection happen during startup
        CategoryCache.update();
    }

    private void loadHooks() {
        getConsole().info("Configuring Hooks...");

        if (Config.i().getHooks().isEcoItems()) {
            getHookManager().registerHook(new EcoItemsHook());
        }

        if (Config.i().getHooks().getDiscord().isEnabled()) {
            getHookManager().registerHook(new DiscordHook());
        }

        getConsole().info("Hooked into %s plugins!".formatted(getHookManager().hookCount()));
    }

    private void loadBroker() {
        Config.Broker settings = Config.i().getBroker();
        if (settings.isEnabled()) {
            getConsole().info("Connecting to Broker...");
            getConsole().info("Broker Type: %s".formatted(settings.getType().getDisplayName()));
            if (Config.i().getDatabase().getType() == DatabaseType.SQLITE) {
                getConsole().severe("------------------------------------------");
                getConsole().severe("Broker has not been enabled as the selected");
                getConsole().severe("       database is not compatible!");
                getConsole().severe("------------------------------------------");
                return;
            }
            broker = switch (settings.getType()) {
                case REDIS -> new RedisBroker(this);
            };
            broker.connect();
            getConsole().info("Successfully connected to broker!");
            return;
        }
        getConsole().info("Not connecting to broker. (Not Enabled)");
    }

    private void loadMigrators() {
        getConsole().info("Loading migrators...");

        migratorManager = new MigratorManager();

        if (getServer().getPluginManager().getPlugin("zAuctionHouseV3") != null) {
            migratorManager.loadMigrator(new zAuctionHouseMigrator());
        }

        if (getServer().getPluginManager().getPlugin("AuctionHouse") != null) {
            migratorManager.loadMigrator(new AuctionHouseMigrator());
        }

        getConsole().info("%s Migrators Loaded!".formatted(migratorManager.getMigratorNames().size()));
    }

    private void setupMetrics() {
        getConsole().info("Starting Metrics...");

        metrics = new Metrics(this, METRICS_ID);
        metrics.addCustomChart(new Metrics.SingleLineChart("items_listed", () -> ListingCache.getListings().size()));

        getConsole().info("Metrics Logging Started!");
    }

    private void initLogger() {
        getConsole().info("Initialising transaction logger...");

        if (!Config.i().isLogToFile()) {
            return;
        }
        try {
            File logsFolder = new File(this.getDataFolder(), "logs");
            if (!logsFolder.exists()) {
                if (!logsFolder.mkdirs()) {
                    getConsole().warning("Failed to create logs folder!");
                    return;
                }
            }

            File logFile = new File(logsFolder, "transaction-log.log");
            if (logFile.exists()) {
                long epochMillis = System.currentTimeMillis();
                String newFileName = "transaction-log_" + epochMillis + ".log";
                File renamedFile = new File(logsFolder, newFileName);
                if (!logFile.renameTo(renamedFile)) {
                    getConsole().warning("Could not rename logfile!");
                }
            }

            FileHandler fileHandler = new FileHandler(logFile.getAbsolutePath());
            fileHandler.setFormatter(new TransactionLogFormatter());
            transactionLogger.setUseParentHandlers(false);
            transactionLogger.addHandler(fileHandler);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        getConsole().info("Logger Started!");
    }

    private void checkForUpdates() {
        final UpdateChecker checker = UpdateChecker.builder()
                .currentVersion(pluginVersion)
                .endpoint(UpdateChecker.Endpoint.SPIGOT)
                .resource(Integer.toString(SPIGOT_ID))
                .build();
        checker.check().thenAccept(checked -> {
            if (checked.isUpToDate()) {
                return;
            }
            Bukkit.getConsoleSender().sendMessage(StringUtils.colorize("&f[Fadah] Fadah is &#D63C3COUTDATED&f! " +
                    "&7Current: &#D63C3C%s &7Latest: &#18D53A%s".formatted(checked.getCurrentVersion(), checked.getLatestVersion())));
        });
    }

    public CompletableFuture<Void> loadPlayerData(UUID uuid) {
        return CompletableFuture.supplyAsync(() -> {
            Optional<CollectionBox> collectionBox = DatabaseManager.getInstance().get(CollectionBox.class, uuid).join();
            collectionBox.ifPresent(list -> CollectionBoxCache.update(uuid, list.collectableItems()));

            Optional<ExpiredItems> expiredItems = DatabaseManager.getInstance().get(ExpiredItems.class, uuid).join();
            expiredItems.ifPresent(list -> ExpiredListingsCache.update(uuid, list.collectableItems()));

            Optional<History> history = DatabaseManager.getInstance().get(History.class, uuid).join();
            history.ifPresent(list -> HistoricItemsCache.update(uuid, list.collectableItems()));

            return null;
        });
    }

    public void reload() {
        FastInvManager.closeAll(this);
        Config.reload();
        Lang.reload();
        Fadah.getINSTANCE().getMenusFile().load();
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.MAIN);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.NEW_LISTING);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.PROFILE);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.EXPIRED_LISTINGS);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.ACTIVE_LISTINGS);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.COLLECTION_BOX);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.CONFIRM_PURCHASE);
        Fadah.getINSTANCE().getLayoutManager().reloadLayout(LayoutManager.MenuType.HISTORY);
        Fadah.getINSTANCE().getCategoriesFile().load();
        CategoryCache.update();
    }
}

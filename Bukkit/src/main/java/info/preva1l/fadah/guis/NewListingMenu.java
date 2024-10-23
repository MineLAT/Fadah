package info.preva1l.fadah.guis;

import com.github.puregero.multilib.MultiLib;
import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.api.ListingCreateEvent;
import info.preva1l.fadah.cache.CategoryCache;
import info.preva1l.fadah.cache.ListingCache;
import info.preva1l.fadah.config.Config;
import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.config.ListHelper;
import info.preva1l.fadah.config.Tuple;
import info.preva1l.fadah.currency.Currency;
import info.preva1l.fadah.currency.CurrencyRegistry;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.data.PermissionsData;
import info.preva1l.fadah.filters.Restrictions;
import info.preva1l.fadah.hooks.impl.DiscordHook;
import info.preva1l.fadah.multiserver.Message;
import info.preva1l.fadah.multiserver.Payload;
import info.preva1l.fadah.records.CurrentListing;
import info.preva1l.fadah.records.Listing;
import info.preva1l.fadah.utils.StringUtils;
import info.preva1l.fadah.utils.TaskManager;
import info.preva1l.fadah.utils.TimeUtil;
import info.preva1l.fadah.utils.guis.*;
import info.preva1l.fadah.utils.logging.TransactionLogger;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickEvent;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.milkbowl.vault.economy.Economy;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.entity.Player;
import org.bukkit.event.inventory.InventoryCloseEvent;
import org.bukkit.inventory.ItemFlag;
import org.bukkit.inventory.ItemStack;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class NewListingMenu extends FastInv {
    private final Fadah plugin = Fadah.getINSTANCE();
    private final Player player;
    private ItemStack itemToSell;
    private Instant timeToDelete;
    private boolean listingStarted = false;
    private boolean advertise = Config.i().getListingAdverts().isEnabledByDefault();
    private boolean isBidding = false;
    private Currency currency;

    public NewListingMenu(Player player, double price) {
        super(LayoutManager.MenuType.NEW_LISTING.getLayout().guiSize(),
                LayoutManager.MenuType.NEW_LISTING.getLayout().guiTitle(), LayoutManager.MenuType.NEW_LISTING);
        this.player = player;
        this.itemToSell = player.getInventory().getItemInMainHand().clone();
        this.timeToDelete = Instant.now().plus(2, ChronoUnit.DAYS);
        this.currency = CurrencyRegistry.get(Config.i().getCurrency().getDefaultCurrency());
        List<Integer> fillerSlots = getLayout().fillerSlots();
        if (!fillerSlots.isEmpty()) {
            setItems(fillerSlots.stream().mapToInt(Integer::intValue).toArray(),
                    GuiHelper.constructButton(GuiButtonType.BORDER));
        }

        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_START, -1),
                new ItemBuilder(getLang().getAsMaterial("create.icon", Material.EMERALD))
                        .name(getLang().getStringFormatted("create.name", "&aClick to create listing!"))
                        .modelData(getLang().getInt("create.model-data"))
                        .addLore(getLang().getLore("create.lore",
                                new DecimalFormat(Config.i().getDecimalFormat())
                                        .format(price)))
                        .setAttributes(null)
                        .flags(ItemFlag.HIDE_ATTRIBUTES)
                        .build(), e -> startListing(timeToDelete, price));
        setClock();
        setAdvertButton();
        setCurrencyButton();
        //setModeButton();
        addNavigationButtons();

        MultiLib.getEntityScheduler(player).execute(plugin,
                () -> player.getInventory().setItemInMainHand(new ItemStack(Material.AIR)),
                () -> this.itemToSell = new ItemStack(Material.AIR),
                0L);
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_ITEM, -1), itemToSell);
    }

    @Override
    protected void onClose(InventoryCloseEvent event) {
        super.onClose(event);
        if (!listingStarted) player.getInventory().setItemInMainHand(itemToSell);
    }

    private void setClock() {
        removeItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_TIME, -1));
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_TIME, -1),
                new ItemBuilder(getLang().getAsMaterial("time.icon", Material.CLOCK))
                        .name(getLang().getStringFormatted("time.name", "&aTime for listing to be active"))
                        .setAttributes(null)
                        .flags(ItemFlag.HIDE_ATTRIBUTES)
                        .addLore(getLang().getLore("time.lore", TimeUtil.formatTimeUntil(timeToDelete.toEpochMilli()))).build(), e -> {
            if (e.isRightClick()) {
                if (e.isShiftClick()) {
                    if (timeToDelete.minus(30, ChronoUnit.MINUTES).toEpochMilli() <= Instant.now().toEpochMilli())
                        return;
                    timeToDelete = timeToDelete.minus(30, ChronoUnit.MINUTES);
                    setClock();
                    return;
                }
                if (timeToDelete.minus(1, ChronoUnit.HOURS).toEpochMilli() <= Instant.now().toEpochMilli()) return;
                timeToDelete = timeToDelete.minus(1, ChronoUnit.HOURS);
                setClock();
            }

            if (e.isLeftClick()) {
                if (e.isShiftClick()) {
                    if (timeToDelete.plus(30, ChronoUnit.MINUTES).toEpochMilli() > Instant.now().plus(10, ChronoUnit.DAYS).toEpochMilli())
                        return;
                    timeToDelete = timeToDelete.plus(30, ChronoUnit.MINUTES);
                    setClock();
                    return;
                }
                if (timeToDelete.plus(1, ChronoUnit.HOURS).toEpochMilli() > Instant.now().plus(10, ChronoUnit.DAYS).toEpochMilli())
                    return;
                timeToDelete = timeToDelete.plus(1, ChronoUnit.HOURS);
                setClock();
            }
        });
    }

    private void setAdvertButton() {
        String postAdvert = StringUtils.formatPlaceholders(advertise
                        ? getLang().getStringFormatted("advert.options.selected", "&8> &e{0}")
                        : getLang().getStringFormatted("advert.options.not-selected", "&f{0}"),
                Lang.i().getAdvertActions().getPost());
        String dontPost = StringUtils.formatPlaceholders(!advertise
                        ? getLang().getStringFormatted("advert.options.selected", "&8> &e{0}")
                        : getLang().getStringFormatted("advert.options.not-selected", "&f{0}"),
                Lang.i().getAdvertActions().getSilent());

        removeItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_ADVERT, -1));
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_ADVERT, -1),
                new ItemBuilder(getLang().getAsMaterial("advert.icon", Material.OAK_SIGN))
                        .name(getLang().getStringFormatted("advert.name", "&eAdvertise Listing"))
                        .modelData(getLang().getInt("advert.model-data"))
                        .setAttributes(null)
                        .flags(ItemFlag.HIDE_ATTRIBUTES)
                        .lore(getLang().getLore("advert.lore",
                                new DecimalFormat(Config.i().getDecimalFormat())
                                        .format(PermissionsData.getHighestDouble(PermissionsData.PermissionType.ADVERT_PRICE, player)),
                                postAdvert, dontPost)).build(), e -> {
                    this.advertise = !advertise;
                    setAdvertButton();
                }
        );
    }

    private void setCurrencyButton() {
        Currency previousCurrency = CurrencyRegistry.getPrevious(currency);
        String previous = previousCurrency == null
                ? Lang.i().getWords().getNone()
                : previousCurrency.getName();
        String current = currency.getName();
        Currency nextCurrency = CurrencyRegistry.getNext(currency);
        String next = nextCurrency == null
                ? Lang.i().getWords().getNone()
                : nextCurrency.getName();
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.CURRENCY, -1),
                new ItemBuilder(getLang().getAsMaterial("currency.icon", Material.GOLD_INGOT))
                        .name(getLang().getStringFormatted("currency.name", "&aCurrency"))
                        .modelData(getLang().getInt("currency.model-data"))
                        .setAttributes(null)
                        .flags(ItemFlag.HIDE_ATTRIBUTES)
                        .lore(getLang().getLore("currency.lore",
                                StringUtils.colorize(previous),
                                StringUtils.colorize(current),
                                StringUtils.colorize(next))).build(), e -> {
            if (e.getClick().isLeftClick() && previousCurrency != null) {
                currency = previousCurrency;
            }

            if (e.getClick().isRightClick() && nextCurrency != null) {
                currency = nextCurrency;
            }

            setCurrencyButton();
        });
    }

    // Not Used (For future bidding update)
    private void setModeButton() {
        String bidding = StringUtils.formatPlaceholders(isBidding
                        ? getLang().getStringFormatted("mode.options.selected", "&8> &e{0}")
                        : getLang().getStringFormatted("mode.options.not-selected", "&f{0}"),
                Lang.i().getWords().getModes().getBidding());
        String bin = StringUtils.formatPlaceholders(!isBidding
                        ? getLang().getStringFormatted("mode.options.selected", "&8> &e{0}")
                        : getLang().getStringFormatted("mode.options.not-selected", "&f{0}"),
                Lang.i().getWords().getModes().getBuyItNow());

        removeItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_MODE, -1));
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.LISTING_MODE, -1),
                new ItemBuilder(getLang().getAsMaterial("mode.icon", Material.HOPPER))
                        .name(getLang().getStringFormatted("mode.name", "&bAuction Mode"))
                        .modelData(getLang().getInt("mode.model-data"))
                        .setAttributes(null)
                        .flags(ItemFlag.HIDE_ATTRIBUTES)
                        .lore(getLang().getLore("mode.lore", bidding, bin)).build(), e -> {
                    this.isBidding = !isBidding;
                    setModeButton();
                }
        );
    }

    private void startListing(Instant deletionDate, double price) {
        String category = CategoryCache.getCategoryForItem(itemToSell);

        if (category == null || Restrictions.isRestrictedItem(itemToSell)) {
            Lang.sendMessage(player, Lang.i().getPrefix() + Lang.i().getErrors().getRestricted());
            return;
        }

        double tax = PermissionsData.getHighestDouble(PermissionsData.PermissionType.LISTING_TAX, player);

        Listing listing = new CurrentListing(UUID.randomUUID(), player.getUniqueId(), player.getName(),
                itemToSell, category, currency.getId(), price, tax, Instant.now().toEpochMilli(), deletionDate.toEpochMilli(), isBidding, Collections.emptyList());

        ListingCreateEvent createEvent = new ListingCreateEvent(player, listing);
        Bukkit.getServer().getPluginManager().callEvent(createEvent);

        if (createEvent.isCancelled()) {
            Lang.sendMessage(player, Lang.i().getPrefix() + createEvent.getCancelReason());
            player.closeInventory();
            return;
        }

        DatabaseManager.getInstance().save(Listing.class, listing);

        if (!Config.i().getBroker().isEnabled()) {
            ListingCache.addListing(listing);
        } else {
            Message.builder()
                    .type(Message.Type.LISTING_ADD)
                    .payload(Payload.withUUID(listing.getId()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        listingStarted = true;

        player.closeInventory();

        double taxAmount = PermissionsData.getHighestDouble(PermissionsData.PermissionType.LISTING_TAX, player);
        String itemName = StringUtils.extractItemName(listing.getItemStack());
        String message = String.join("\n", ListHelper.replace(Lang.i().getNotifications().getNewListing(),
                Tuple.of("%item%", itemName),
                Tuple.of("%price%", new DecimalFormat(Config.i().getDecimalFormat()).format(listing.getPrice())),
                Tuple.of("%time%", TimeUtil.formatTimeUntil(listing.getDeletionDate())),
                Tuple.of("%current_listings%", PermissionsData.getCurrentListings(player) + ""),
                Tuple.of("%max_listings%", PermissionsData.getHighestInt(PermissionsData.PermissionType.MAX_LISTINGS, player) + ""),
                Tuple.of("%tax%", taxAmount + ""),
                Tuple.of("%price_after_tax%", new DecimalFormat(Config.i().getDecimalFormat()).format((taxAmount / 100) * price))
        ));
        Lang.sendMessage(player, message);

        TransactionLogger.listingCreated(listing);

        Config.Hooks.Discord discConf = Config.i().getHooks().getDiscord();
        if ((discConf.isEnabled() && plugin.getHookManager().getHook(DiscordHook.class).isPresent()) &&
                ((discConf.isEnabled() && advertise)
                        || !discConf.isOnlySendOnAdvert())) {
            plugin.getHookManager().getHook(DiscordHook.class).get().send(listing);
        }

        if (advertise) {
            double advertPrice = PermissionsData.getHighestDouble(PermissionsData.PermissionType.ADVERT_PRICE, player);
            if (!listing.getCurrency().canAfford(player, advertPrice)) {
                Lang.sendMessage(player,Lang.i().getPrefix() + Lang.i().getErrors().getAdvertExpense());
                return;
            }

            listing.getCurrency().withdraw(player, advertPrice);

            String advertMessage = String.join("&r\n", ListHelper.replace(Lang.i().getNotifications().getAdvert(),
                    Tuple.of("%player%", player.getName()),
                    Tuple.of("%item%", itemName),
                    Tuple.of("%price%", new DecimalFormat(Config.i().getDecimalFormat()).format(listing.getPrice()))
            ));

            if (!Config.i().getBroker().isEnabled()) {
                TaskManager.Async.run(Fadah.getINSTANCE(), () -> {
                    Component textComponent = MiniMessage.miniMessage().deserialize(StringUtils.legacyToMiniMessage(advertMessage));
                    textComponent = textComponent.clickEvent(ClickEvent.clickEvent(ClickEvent.Action.RUN_COMMAND, "/ah view-listing " + listing.getId()));
                    for (Player announce : Bukkit.getOnlinePlayers()) {
                        Fadah.getINSTANCE().getAdventureAudience().player(announce).sendMessage(textComponent);
                    }
                });
            } else {
                Message.builder()
                        .type(Message.Type.BROADCAST)
                        .payload(Payload.withBroadcast(advertMessage, "/ah view-listing " + listing.getId()))
                        .build().send(Fadah.getINSTANCE().getBroker());
            }
        }
    }

    private void addNavigationButtons() {
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.CLOSE, 49),
                GuiHelper.constructButton(GuiButtonType.CLOSE), e -> e.getWhoClicked().closeInventory());
    }
}

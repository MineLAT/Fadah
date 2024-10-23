package info.preva1l.fadah.records;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.api.ListingEndEvent;
import info.preva1l.fadah.api.ListingEndReason;
import info.preva1l.fadah.api.ListingPurchaseEvent;
import info.preva1l.fadah.cache.CollectionBoxCache;
import info.preva1l.fadah.cache.ExpiredListingsCache;
import info.preva1l.fadah.cache.ListingCache;
import info.preva1l.fadah.config.Config;
import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.config.ListHelper;
import info.preva1l.fadah.config.Tuple;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.data.DatabaseType;
import info.preva1l.fadah.multiserver.Message;
import info.preva1l.fadah.multiserver.Payload;
import info.preva1l.fadah.utils.TaskManager;
import info.preva1l.fadah.utils.logging.TransactionLogger;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.inventory.ItemStack;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class CurrentListing extends Listing {

    public CurrentListing(@NotNull UUID id, @NotNull UUID owner, @NotNull String ownerName,
                          @NotNull ItemStack itemStack, @NotNull String categoryID, @NotNull String currency, double price,
                          double tax, long creationDate, long deletionDate, boolean biddable, List<Bid> bids) {
        super(id, owner, ownerName, itemStack, categoryID, currency, price, tax, creationDate, deletionDate, biddable, bids);
    }

    @Override
    public void purchase(@NotNull Player buyer) {
        if (!getCurrency().canAfford(buyer, this.getPrice())) {
            buyer.sendMessage(Lang.i().getPrefix() + Lang.i().getErrors().getTooExpensive());
            return;
        }
        if (ListingCache.getListing(this.getId()) == null) { // todo: readd strict checks
            buyer.sendMessage(Lang.i().getPrefix() + Lang.i().getErrors().getDoesNotExist());
            return;
        }
        // Money Transfer
        getCurrency().withdraw(buyer, this.getPrice());
        double taxed = (this.getTax()/100) * this.getPrice();
        getCurrency().add(Bukkit.getOfflinePlayer(this.getOwner()), this.getPrice() - taxed);

        // Remove Listing
        if (!Config.i().getBroker().isEnabled()) {
            ListingCache.removeListing(this);
        } else {
            Message.builder()
                    .type(Message.Type.LISTING_REMOVE)
                    .payload(Payload.withUUID(this.getId()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }
        if (Config.i().getDatabase().getType() == DatabaseType.MONGO) {
            DatabaseManager.getInstance().delete(Listing.class, this);
        }

        // Add to collection box
        ItemStack itemStack = this.getItemStack().clone();
        CollectableItem collectableItem = new CollectableItem(this.getId(), this.getOwner(), itemStack, Instant.now().toEpochMilli());
        CollectionBox box = CollectionBox.of(buyer.getUniqueId());
        box.collectableItems().add(collectableItem);
        DatabaseManager.getInstance().save(CollectionBox.class, box);

        // Send Cache Updates
        if (!Config.i().getBroker().isEnabled()) {
            CollectionBoxCache.addItem(buyer.getUniqueId(), collectableItem);
        } else {
            Message.builder()
                    .type(Message.Type.COLLECTION_BOX_UPDATE)
                    .payload(Payload.withUUID(buyer.getUniqueId()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        // Notify Both Players
        Lang.sendMessage(buyer, String.join("\n", Lang.i().getNotifications().getNewItem()));

        String itemName = this.getItemStack().getItemMeta().getDisplayName().isBlank() ?
                this.getItemStack().getType().name() : this.getItemStack().getItemMeta().getDisplayName();
        String formattedPrice = new DecimalFormat(Config.i().getDecimalFormat()).format(this.getPrice() - taxed);
        String message = String.join("\n", ListHelper.replace(Lang.i().getNotifications().getSale(),
                Tuple.of("%item%", itemName),
                Tuple.of("%price%", formattedPrice)));

        Player seller = Bukkit.getPlayer(this.getOwner());
        if (seller != null) {
            Lang.sendMessage(seller, message);
        } else {
            Message.builder()
                    .type(Message.Type.NOTIFICATION)
                    .payload(Payload.withNotification(this.getOwner(), message));
        }

        TransactionLogger.listingSold(this, buyer);

        Bukkit.getServer().getPluginManager().callEvent(new ListingPurchaseEvent(this.getAsStale(), buyer));
    }

    @Override
    public boolean cancel(@NotNull Player canceller) {
        if (ListingCache.getListing(this.getId()) == null) { // todo: re-add strict checks
            Lang.sendMessage(canceller, Lang.i().getPrefix() + Lang.i().getErrors().getDoesNotExist());
            return false;
        }
        Lang.sendMessage(canceller, Lang.i().getPrefix() + Lang.i().getNotifications().getCancelled());
        if (!Config.i().getBroker().isEnabled()) {
            ListingCache.removeListing(this);
        } else {
            Message.builder()
                    .type(Message.Type.LISTING_REMOVE)
                    .payload(Payload.withUUID(this.getId()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }
        if (Config.i().getDatabase().getType() == DatabaseType.MONGO) {
            DatabaseManager.getInstance().delete(Listing.class, this);
        }


        CollectableItem collectableItem = new CollectableItem(this.getId(), this.getOwner(), this.getItemStack(), Instant.now().toEpochMilli());
        ExpiredItems items = ExpiredItems.of(getOwner());
        items.collectableItems().add(collectableItem);
        DatabaseManager.getInstance().save(ExpiredItems.class, items);
        if (!Config.i().getBroker().isEnabled()) {
            ExpiredListingsCache.addItem(getOwner(), collectableItem);
        } else {
            Message.builder()
                    .type(Message.Type.EXPIRED_LISTINGS_UPDATE)
                    .payload(Payload.withUUID(this.getOwner()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        boolean isAdmin = !this.isOwner(canceller);
        TransactionLogger.listingRemoval(this, isAdmin);
        TaskManager.Async.run(Fadah.getINSTANCE(), () ->
                Bukkit.getServer().getPluginManager().callEvent(
                        new ListingEndEvent(this, isAdmin
                                ? ListingEndReason.CANCELLED_ADMIN
                                : ListingEndReason.CANCELLED)
                )
        );
        return true;
    }

    public StaleListing getAsStale() {
        return new StaleListing(id, owner, ownerName, itemStack, categoryID, currencyId, price, tax, creationDate, deletionDate, biddable, bids);
    }
}

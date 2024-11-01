package info.preva1l.fadah.utils.logging;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.config.Config;
import info.preva1l.fadah.hooks.impl.InfluxDBHook;
import info.preva1l.fadah.multiserver.Message;
import info.preva1l.fadah.multiserver.Payload;
import info.preva1l.fadah.records.Listing;
import info.preva1l.fadah.utils.StringUtils;
import lombok.experimental.UtilityClass;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import java.util.Optional;

@UtilityClass
public class TransactionLogger {

    public void listingCreated(Listing listing) {
        // In game logs
        if (Config.i().getBroker().isEnabled()) {
            Message.builder()
                    .type(Message.Type.HISTORY_UPDATE)
                    .payload(Payload.withUUID(listing.getOwner()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        // Log file logs
        String logMessage = StringUtils.formatPlaceholders("[NEW LISTING] Seller: {0} ({1}), Price: {2}, ItemStack: {3}",
                Bukkit.getOfflinePlayer(listing.getOwner()).getName(), Bukkit.getOfflinePlayer(listing.getOwner()).getUniqueId().toString(),
                listing.getPrice(), listing.getItemStack().toString());

        Fadah.getINSTANCE().getTransactionLogger().info(logMessage);
        Optional<InfluxDBHook> hook = Fadah.getINSTANCE().getHookManager().getHook(InfluxDBHook.class);
        if (Config.i().getHooks().getInfluxdb().isEnabled() && hook.isPresent() && hook.get().isEnabled()) {
            hook.get().log(logMessage);
        }
    }

    public void listingSold(Listing listing, Player buyer) {
        // In Game logs
        if (Config.i().getBroker().isEnabled()) {
            Message.builder()
                    .type(Message.Type.HISTORY_UPDATE)
                    .payload(Payload.withUUID(listing.getOwner()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        if (Config.i().getBroker().isEnabled()) {
            Message.builder()
                    .type(Message.Type.HISTORY_UPDATE)
                    .payload(Payload.withUUID(buyer.getUniqueId()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        // Log file logs
        String logMessage = StringUtils.formatPlaceholders("[LISTING SOLD] Seller: {0} ({1}), Buyer: {2} ({3}), Price: {4}, ItemStack: {5}",
                listing.getOwnerName(), listing.getOwner(), buyer.getName(), buyer.getUniqueId(), listing.getPrice(), listing.getItemStack());
        Fadah.getINSTANCE().getTransactionLogger().info(logMessage);
        Optional<InfluxDBHook> hook = Fadah.getINSTANCE().getHookManager().getHook(InfluxDBHook.class);
        if (Config.i().getHooks().getInfluxdb().isEnabled() && hook.isPresent() && hook.get().isEnabled()) {
            hook.get().log(logMessage);
        }
    }

    public void listingRemoval(Listing listing, boolean isAdmin) {
        // In game logs
        if (Config.i().getBroker().isEnabled()) {
            Message.builder()
                    .type(Message.Type.HISTORY_UPDATE)
                    .payload(Payload.withUUID(listing.getOwner()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        // Log file logs
        String logMessage = StringUtils.formatPlaceholders("[LISTING REMOVED] Seller: {0} ({1}), Price: {2}, ItemStack: {3}, byAdmin: {4}",
                Bukkit.getOfflinePlayer(listing.getOwner()).getName(), Bukkit.getOfflinePlayer(listing.getOwner()).getUniqueId().toString(),
                listing.getPrice(), listing.getItemStack().toString(), isAdmin);
        Fadah.getINSTANCE().getTransactionLogger().info(logMessage);
        Optional<InfluxDBHook> hook = Fadah.getINSTANCE().getHookManager().getHook(InfluxDBHook.class);
        if (Config.i().getHooks().getInfluxdb().isEnabled() && hook.isPresent() && hook.get().isEnabled()) {
            hook.get().log(logMessage);
        }
    }

    public void listingExpired(Listing listing) {
        // In game logs
        if (Config.i().getBroker().isEnabled()) {
            Message.builder()
                    .type(Message.Type.HISTORY_UPDATE)
                    .payload(Payload.withUUID(listing.getOwner()))
                    .build().send(Fadah.getINSTANCE().getBroker());
        }

        // Log file logs
        String logMessage = StringUtils.formatPlaceholders("[LISTING EXPIRED] Seller: {0} ({1}), Price: {2}, ItemStack: {3}",
                Bukkit.getOfflinePlayer(listing.getOwner()).getName(), Bukkit.getOfflinePlayer(listing.getOwner()).getUniqueId().toString(),
                listing.getPrice(), listing.getItemStack().toString());
        Fadah.getINSTANCE().getTransactionLogger().info(logMessage);
        Optional<InfluxDBHook> hook = Fadah.getINSTANCE().getHookManager().getHook(InfluxDBHook.class);
        if (Config.i().getHooks().getInfluxdb().isEnabled() && hook.isPresent() && hook.get().isEnabled()) {
            hook.get().log(logMessage);
        }
    }
}

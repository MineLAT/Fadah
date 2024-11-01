package info.preva1l.fadah.cache;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.guis.MainMenu;
import info.preva1l.fadah.guis.ViewListingsMenu;
import info.preva1l.fadah.records.Listing;
import info.preva1l.fadah.utils.guis.FastInv;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@UtilityClass
public final class ListingCache {
    private CompletableFuture<Map<UUID, @NotNull Listing>> listings = listings();

    @NotNull
    private CompletableFuture<Map<UUID, @NotNull Listing>> listings() {
        return DatabaseManager.getInstance().getAll(Listing.class).thenApply(listings -> {
            final Map<UUID, Listing> temp = new ConcurrentHashMap<>();
            for (Listing listing : listings) {
                temp.put(listing.getId(), listing);
            }
            Fadah.getConsole().info("Loaded " + temp.size() + " listings from database");
            return temp;
        });
    }

    @Nullable
    public Listing addListing(@Nullable Listing newListing) {
        if (newListing == null) {
            return null;
        }
        return getListings().put(newListing.getId(), newListing);
    }

    @Nullable
    public Listing removeListing(@NotNull Listing listing) {
        return removeListing(listing.getId());
    }

    @Nullable
    public Listing removeListing(@NotNull UUID id) {
        return getListings().remove(id);
    }

    @Nullable
    public Listing getListing(@NotNull UUID id) {
        return getListings().get(id);
    }

    public void update() {
        listings = listings();
        FastInv.update(MainMenu.class);
        FastInv.update(ViewListingsMenu.class);
    }

    @NotNull
    public Map<UUID, Listing> getListings() {
        return listings.join();
    }
}
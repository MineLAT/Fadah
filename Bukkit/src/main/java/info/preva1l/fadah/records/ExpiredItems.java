package info.preva1l.fadah.records;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.data.DatabaseManager;

import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public record ExpiredItems(
        UUID owner,
        LinkedHashSet<CollectableItem> collectableItems
) {
    public static CompletableFuture<ExpiredItems> of(UUID owner) {
        return DatabaseManager.getInstance().get(ExpiredItems.class, owner)
                .thenApply(optional -> {
                    final ExpiredItems expired = optional.orElseGet(() -> new ExpiredItems(owner, new LinkedHashSet<>()));
                    Fadah.getConsole().info("Loaded " + expired.collectableItems().size() + " expired items for user " + expired.owner());
                    return expired;
                });
    }

    public static ExpiredItems of(CollectableItem item) {
        return new ExpiredItems(item.owner(), new LinkedHashSet<>() {{ add(item); }});
    }
}

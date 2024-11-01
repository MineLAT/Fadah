package info.preva1l.fadah.records;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.data.DatabaseManager;

import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public record CollectionBox(
        UUID owner,
        LinkedHashSet<CollectableItem> collectableItems
) {
    public static CompletableFuture<CollectionBox> of(UUID owner) {
        return DatabaseManager.getInstance().get(CollectionBox.class, owner)
                .thenApply(optional -> {
                    final CollectionBox box = optional.orElseGet(() -> new CollectionBox(owner, new LinkedHashSet<>()));
                    Fadah.getConsole().info("Loaded " + box.collectableItems().size() + " collectable items for user " + box.owner());
                    return box;
                });
    }

    public static CollectionBox of(UUID owner, CollectableItem item) {
        return new CollectionBox(owner, new LinkedHashSet<>() {{ add(item); }});
    }
}

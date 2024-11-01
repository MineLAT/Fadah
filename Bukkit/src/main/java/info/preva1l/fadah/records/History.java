package info.preva1l.fadah.records;

import info.preva1l.fadah.data.DatabaseManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public record History(
        UUID owner,
        List<HistoricItem> collectableItems
) {
    public static CompletableFuture<History> of(UUID owner) {
        return DatabaseManager.getInstance().get(History.class, owner)
                .thenApply(optional -> optional.orElseGet(() -> new History(owner, new ArrayList<>())));
    }
}

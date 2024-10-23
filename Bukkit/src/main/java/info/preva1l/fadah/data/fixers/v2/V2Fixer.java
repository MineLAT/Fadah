package info.preva1l.fadah.data.fixers.v2;

import java.util.UUID;

public interface V2Fixer {

    V2Fixer EMPTY = new V2Fixer() { };

    default void fixExpiredItems(UUID player) {
        // empty default method
    }
    default void fixCollectionBox(UUID player) {
        // empty default method
    }
    default boolean needsFixing(UUID player) {
        return false;
    }
}

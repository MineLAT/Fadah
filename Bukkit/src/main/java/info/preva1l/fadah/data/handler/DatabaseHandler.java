package info.preva1l.fadah.data.handler;

import info.preva1l.fadah.data.fixers.v2.V2Fixer;

import java.util.UUID;

public interface DatabaseHandler extends DataHandler {
    boolean isConnected();
    void connect();
    void destroy();
    void registerDaos();
    void wipeDatabase();


    default V2Fixer getV2Fixer() {
        return V2Fixer.EMPTY;
    }
    default void fixData(UUID player) {
        getV2Fixer().fixCollectionBox(player);
        getV2Fixer().fixExpiredItems(player);
    }

    default boolean needsFixing(UUID player) {
        return getV2Fixer().needsFixing(player);
    }
}
package info.preva1l.fadah.listeners;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.utils.StringUtils;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class PlayerListener implements Listener {
    private final List<UUID> loading = new CopyOnWriteArrayList<>();

    @EventHandler
    public void joinListener(AsyncPlayerPreLoginEvent e) {
        if (!DatabaseManager.getInstance().isConnected()) {
            e.setLoginResult(AsyncPlayerPreLoginEvent.Result.KICK_OTHER);
            e.setKickMessage(StringUtils.colorize(Lang.i().getPrefix() + Lang.i().getErrors().getDatabaseLoading()));
            return;
        }

        loading.add(e.getUniqueId());
        Fadah.getINSTANCE().loadPlayerData(e.getUniqueId()).join();
        loading.remove(e.getUniqueId());
    }

    @EventHandler
    public void finalJoin(PlayerJoinEvent e) {
        if (loading.contains(e.getPlayer().getUniqueId())) {
            e.getPlayer().kickPlayer(StringUtils.colorize(Lang.i().getPrefix() + Lang.i().getErrors().getDatabaseLoading()));
        }
    }
}

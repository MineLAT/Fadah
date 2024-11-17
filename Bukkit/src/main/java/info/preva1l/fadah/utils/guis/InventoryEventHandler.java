package info.preva1l.fadah.utils.guis;

import com.github.puregero.multilib.regionized.RegionizedTask;
import lombok.experimental.UtilityClass;
import org.bukkit.inventory.Inventory;

import java.util.HashMap;
import java.util.Map;

@UtilityClass
public class InventoryEventHandler {
    public final Map<Inventory, RegionizedTask> tasksToQuit = new HashMap<>();
}

package info.preva1l.fadah.guis;

import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.records.CollectableItem;
import info.preva1l.fadah.records.ExpiredItems;
import info.preva1l.fadah.utils.StringUtils;
import info.preva1l.fadah.utils.TimeUtil;
import info.preva1l.fadah.utils.guis.*;
import lombok.Getter;
import org.bukkit.OfflinePlayer;
import org.bukkit.entity.Player;

import java.util.LinkedHashSet;
import java.util.List;

public class ExpiredListingsMenu extends PaginatedFastInv {
    private final Player viewer;
    @Getter
    private final OfflinePlayer owner;
    private final LinkedHashSet<CollectableItem> expiredItems;

    public ExpiredListingsMenu(Player viewer, OfflinePlayer owner, int page) {
        super(LayoutManager.MenuType.EXPIRED_LISTINGS.getLayout().guiSize(),
                LayoutManager.MenuType.EXPIRED_LISTINGS.getLayout().formattedTitle(viewer.getUniqueId() == owner.getUniqueId()
                        ? StringUtils.capitalize(Lang.i().getWords().getYour())
                        : owner.getName()+"'s", owner.getName()+"'s"), viewer, LayoutManager.MenuType.EXPIRED_LISTINGS,
                List.of(10, 11, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 24, 25, 28, 29, 30, 31, 32, 33, 34));
        this.viewer = viewer;
        this.owner = owner;
        this.page = page;
        this.expiredItems = ExpiredItems.of(owner.getUniqueId()).join().collectableItems();

        List<Integer> fillerSlots = getLayout().fillerSlots();
        if (!fillerSlots.isEmpty()) {
            setItems(fillerSlots.stream().mapToInt(Integer::intValue).toArray(),
                    GuiHelper.constructButton(GuiButtonType.BORDER));
        }
        setPaginationMappings(getLayout().paginationSlots());

        addNavigationButtons();
        fillPaginationItems();
        populatePage();
        addPaginationControls();
    }

    @Override
    protected void onUpdate() {
        this.expiredItems.clear();
        this.expiredItems.addAll(ExpiredItems.of(owner.getUniqueId()).join().collectableItems());
    }

    @Override
    protected synchronized void fillPaginationItems() {
        for (CollectableItem collectableItem : expiredItems) {
            ItemBuilder itemStack = new ItemBuilder(collectableItem.itemStack().clone())
                    .addLore(getLang().getLore("lore", TimeUtil.formatTimeSince(collectableItem.dateAdded())));

            addPaginationItem(new PaginatedItem(itemStack.build(), e -> {
                int slot = viewer.getInventory().firstEmpty();
                if (slot == -1) {

                    Lang.sendMessage(viewer, Lang.i().getPrefix() + Lang.i().getErrors().getInventoryFull());
                    return;
                }
                if (!expiredItems.remove(collectableItem)) {
                    Lang.sendMessage(viewer, StringUtils.colorize(Lang.i().getPrefix() + Lang.i().getErrors().getDoesNotExist()));
                    return;
                }
                DatabaseManager.getInstance().deleteSpecific(ExpiredItems.class, new ExpiredItems(owner.getUniqueId(), expiredItems), collectableItem).thenRun(() -> {
                    viewer.getInventory().setItem(slot, collectableItem.itemStack());

                    updatePagination();
                });
            }));
        }
    }

    @Override
    protected void addPaginationControls() {
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.PAGINATION_CONTROL_ONE, -1),
                GuiHelper.constructButton(GuiButtonType.BORDER));
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.PAGINATION_CONTROL_TWO,-1),
                GuiHelper.constructButton(GuiButtonType.BORDER));
        if (page > 0) {
            setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.PAGINATION_CONTROL_ONE, -1),
                    GuiHelper.constructButton(GuiButtonType.PREVIOUS_PAGE), e -> previousPage());
        }

        if (expiredItems != null && expiredItems.size() >= index + 1) {
            setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.PAGINATION_CONTROL_TWO,-1),
                    GuiHelper.constructButton(GuiButtonType.NEXT_PAGE), e -> nextPage());
        }
    }

    private void addNavigationButtons() {
        setItem(getLayout().buttonSlots().getOrDefault(LayoutManager.ButtonType.BACK, -1),
                GuiHelper.constructButton(GuiButtonType.BACK), e ->
                new ProfileMenu(viewer, owner).open(viewer));
    }
}

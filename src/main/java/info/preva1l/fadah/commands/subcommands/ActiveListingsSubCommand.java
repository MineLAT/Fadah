package info.preva1l.fadah.commands.subcommands;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.guis.ActiveListingsMenu;
import info.preva1l.fadah.utils.commands.SubCommand;
import info.preva1l.fadah.utils.commands.SubCommandArgs;
import info.preva1l.fadah.utils.commands.SubCommandArguments;
import org.jetbrains.annotations.NotNull;

public class ActiveListingsSubCommand extends SubCommand {
    public ActiveListingsSubCommand(Fadah plugin) {
        super(plugin);
    }

    @SubCommandArgs(name = "active-listings", aliases = {"activelistings", "active"}, permission = "fadah.active-listings", description = "View your Active listings!")
    public void execute(@NotNull SubCommandArguments command) {
        if (!Fadah.getINSTANCE().getConfigFile().getBoolean("enabled")) {
            command.sender().sendMessage(Lang.PREFIX.toFormattedString() + Lang.AUCTION_DISABLED.toFormattedString());
            return;
        }
        assert command.getPlayer() != null;
        new ActiveListingsMenu(command.getPlayer(), 0).open(command.getPlayer());
    }
}

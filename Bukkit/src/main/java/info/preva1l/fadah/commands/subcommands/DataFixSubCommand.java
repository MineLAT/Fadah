package info.preva1l.fadah.commands.subcommands;

import info.preva1l.fadah.Fadah;
import info.preva1l.fadah.config.Lang;
import info.preva1l.fadah.data.DatabaseManager;
import info.preva1l.fadah.utils.commands.SubCommand;
import info.preva1l.fadah.utils.commands.SubCommandArgs;
import info.preva1l.fadah.utils.commands.SubCommandArguments;
import org.jetbrains.annotations.NotNull;

public class DataFixSubCommand extends SubCommand {

    public DataFixSubCommand(Fadah plugin) {
        super(plugin, Lang.i().getCommands().getAbout().getAliases(), Lang.i().getCommands().getAbout().getDescription());
    }

    @SubCommandArgs(name = "datafix", permission = "fadah.datafix", inGameOnly = false)
    public void execute(@NotNull SubCommandArguments command) {
        command.reply("&aRunning manual data fix...");
        DatabaseManager.getInstance().fixAll().whenComplete((value, exception) -> {
            if (exception != null) {
                command.reply("&cThere is an error while trying to fix all the data");
                exception.printStackTrace();
            } else {
                command.reply("&aData fix completed, affected rows: " + value);
            }
        });
    }
}
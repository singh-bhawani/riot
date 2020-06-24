package com.redislabs.riot.redis;

import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.riot.RiotApp;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Slf4j
@Command
public abstract class AbstractRedisCommand extends HelpCommand {

    @CommandLine.ParentCommand
    private RiotApp app;

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        StatefulConnection<String, String> connection = app.connection();
        BaseRedisCommands<String, String> commands = app.sync(connection);
        execute(commands);
    }

    protected abstract void execute(BaseRedisCommands<String, String> commands);

}

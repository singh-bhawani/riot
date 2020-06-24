package com.redislabs.riot;

import com.redislabs.picocliredis.HelpCommand;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.ConnectionPoolConfig;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import picocli.CommandLine;

@Slf4j
@CommandLine.Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractTransferCommand<I, O> extends HelpCommand implements Runnable {

    @CommandLine.ParentCommand
    private RiotApp app;
    @CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @CommandLine.Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;

    protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder) {
        return app.configure(builder);
    }

    protected <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder) {
        return app.configure(builder);
    }

    protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return app.configure(builder, redis);
    }

    protected <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return app.configure(builder, redis);
    }

    public void execute(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        Transfer<I, O> transfer = transfer(reader, processor, writer);
        ProgressBarOptions progressBarOptions = ProgressBarOptions.builder().taskName(taskName()).initialMax(maxItemCount).quiet(app.isQuiet()).build();
        ProgressBarReporter reporter = ProgressBarReporter.builder().transfer(transfer).options(progressBarOptions).build();
        reporter.start();
        transfer.execute();
        reporter.stop();
    }

    protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        return Transfer.<I, O>builder().reader(reader).processor(processor).writer(writer).batchSize(batchSize).threadCount(threads).maxItemCount(maxItemCount).build();
    }

    protected abstract String taskName();

    @Override
    public void run() {
        ItemReader<I> reader;
        try {
            reader = reader();
        } catch (Exception e) {
            log.error("Could not create reader", e);
            return;
        }
        ItemProcessor<I, O> processor;
        try {
            processor = processor();
        } catch (Exception e) {
            log.error("Could not create processor", e);
            return;
        }
        ItemWriter writer;
        try {
            writer = writer();
        } catch (Exception e) {
            log.error("Could not create writer", e);
            return;
        }
        execute(reader, processor, writer);
    }

    protected abstract ItemReader<I> reader() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected abstract ItemWriter<O> writer() throws Exception;


}

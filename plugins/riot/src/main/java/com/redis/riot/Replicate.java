package com.redis.riot;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.Assert;

import com.redis.riot.core.ProcessingItemWriter;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.Step;
import com.redis.riot.meesho.MCacheProcessor;
import com.redis.riot.meesho.McacheIdKeyProcessor;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.ByteArrayCodec;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", aliases = "sync", description = "Replicate a Redis database into another Redis database.")
public class Replicate extends AbstractCompareCommand {

	public enum Type {
		STRUCT, DUMP
	}

	public static final Type DEFAULT_TYPE = Type.DUMP;
	public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;

	private static final String COMPARE_STEP_NAME = "compare";
	private static final String SCAN_TASK_NAME = "Scanning";
	private static final String LIVEONLY_TASK_NAME = "Listening";
	private static final String LIVE_TASK_NAME = "Scanning/Listening";

	@Option(names = "--type", description = "Replication type: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
	private Type type = DEFAULT_TYPE;

	@ArgGroup(exclusive = false)
	private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

	@Option(names = "--log-keys", description = "Log keys being read and written.")
	private boolean logKeys;

	@Option(names = "--no-replace", description = "Skip keys already present in the target instead of overwriting them. Existence is checked per key at write time and is not atomic against concurrent writes to the target. Disables dataset verification (compare).")
	private boolean noReplace;

	private NoReplaceFilter<byte[], byte[]> noReplaceFilter;

	@Option(names = "--ignore-expired", description = "Do not propagate source key expirations to the target. Keys that expire (TTL) on the source are left untouched on the target; explicit deletions still propagate. Useful for long-running syncs where the target is authoritative. Disables dataset verification (compare).")
	private boolean ignoreExpired;

	@Option(names = "--mcache", description = "Enable MCache key/value transformation: prepend --key-prefix to keys and an MCache marker byte to string values.")
	private boolean mcache;

	@Option(names = "--key-prefix", description = "Key prefix to prepend to every key (requires --mcache).", paramLabel = "<prefix>")
	private String keyPrefix = "";

	@Option(names = "--already-has-prefix", description = "Strip an existing leading marker byte from string values before re-adding it (requires --mcache).")
	private boolean alreadyHasPrefix;

	@Option(names = "--source-mcache-id", description = "Strip the 2-byte binary memcoil mcacheId prefix for this id (0..65535) from source keys. Keys without that exact prefix are skipped. Disables dataset verification (compare).", paramLabel = "<id>")
	private Integer sourceMcacheId;

	@Option(names = "--target-mcache-id", description = "Prepend the 2-byte binary memcoil mcacheId prefix for this id (0..65535) to keys. Cannot be combined with --mcache. Disables dataset verification (compare).", paramLabel = "<id>")
	private Integer targetMcacheId;

	@Option(names = "--compare", description = "Compare mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
	private CompareMode compareMode = DEFAULT_COMPARE_MODE;

	@Option(names = "--struct", description = "Enable data structure-specific replication")
	public void setStruct(boolean enable) {
		this.type = enable ? Type.STRUCT : Type.DUMP;
	}

	@Override
	protected boolean isQuickCompare() {
		return compareMode == CompareMode.QUICK;
	}

	@Override
	protected Job job() {
		List<Step<?, ?>> steps = new ArrayList<>();
		Step<KeyValue<byte[]>, KeyValue<byte[]>> step = replicateStep();
		steps.add(step);
		if (shouldCompare()) {
			steps.add(compareStep().name(COMPARE_STEP_NAME));
		}
		return job(steps);
	}

	@Override
	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		super.configureTargetRedisWriter(writer);
		log.info("Configuring target Redis writer with {}", targetRedisWriterArgs);
		targetRedisWriterArgs.configure(writer);
	}

	protected Step<KeyValue<byte[]>, KeyValue<byte[]>> replicateStep() {
		RedisItemReader<byte[], byte[]> reader = reader();
		configureSourceRedisReader(reader);
		Step<KeyValue<byte[]>, KeyValue<byte[]>> step = new ExportStepHelper(log).step(reader, replicateWriter());
		step.processor(filter());
		step.taskName(taskName(reader));
		if (logKeys) {
			log.info("Adding key logger");
			step.writeListener(new ReplicateWriteLogger<>(log, reader.getCodec()));
			ReplicateReadLogger<byte[]> readLogger = new ReplicateReadLogger<>(log, reader.getCodec());
			reader.addItemReadListener(readLogger);
			reader.addItemWriteListener(readLogger);
		}
		return step;
	}

	@Override
	protected ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>> filter() {
		List<ItemProcessor<KeyValue<byte[]>, KeyValue<byte[]>>> processors = new ArrayList<>();
		processors.add(super.filter());
		if (isMcacheIdRewrite()) {
			if (sourceMcacheId != null) {
				McacheIdKeyProcessor.validateId(sourceMcacheId);
			}
			if (targetMcacheId != null) {
				McacheIdKeyProcessor.validateId(targetMcacheId);
				if (mcache) {
					throw new IllegalArgumentException(
							"--target-mcache-id cannot be combined with --mcache (--key-prefix): both prefix target keys");
				}
			}
			log.info("Enabling mcache id key rewrite: sourceMcacheId={} targetMcacheId={}", sourceMcacheId,
					targetMcacheId);
			processors.add(new McacheIdKeyProcessor<>(sourceMcacheId, targetMcacheId, log));
		}
		if (mcache) {
			log.info("Enabling MCache transformation with keyPrefix='{}' alreadyHasPrefix={}", keyPrefix,
					alreadyHasPrefix);
			processors.add(new MCacheProcessor<>(ByteArrayCodec.INSTANCE, log, keyPrefix, alreadyHasPrefix));
		}
		if (ignoreExpired) {
			log.info("Enabling --ignore-expired: source key expirations will not be propagated to the target");
			processors.add(new IgnoreExpiredFilter<>(ByteArrayCodec.INSTANCE, log));
		}
		if (noReplace) {
			log.info("Enabling --no-replace: keys already present in the target will be skipped");
			noReplaceFilter = new NoReplaceFilter<>(getTargetRedisContext().getClient(),
					getTargetRedisContext().isCluster(), ByteArrayCodec.INSTANCE, log);
			processors.add(noReplaceFilter);
		}
		return RiotUtils.processor(processors);
	}

	/**
	 * With an mcache id rewrite, keys carry a binary prefix (e.g. 195 =
	 * {@code [0x00, 0xC3]}) that is not valid UTF-8. The default processing writer
	 * round-trips keys through UTF-8 strings, which would corrupt them, so only
	 * {@code --no-ttl} is supported here and it is applied on byte[] keys.
	 */
	@Override
	protected ItemWriter<KeyValue<byte[]>> processingWriter(ItemWriter<KeyValue<byte[]>> writer) {
		if (!isMcacheIdRewrite()) {
			return super.processingWriter(writer);
		}
		KeyValueProcessorArgs args = getProcessorArgs();
		Assert.isTrue(
				args.getKeyExpression() == null && args.getTypeExpression() == null && args.getTtlExpression() == null
						&& !args.isNoStreamIds() && !args.isPrune(),
				"--key-proc, --type-proc, --ttl-proc, --no-stream-id and --stream-prune cannot be combined with --source-mcache-id/--target-mcache-id (binary keys are not UTF-8 safe); only --no-ttl is supported");
		if (!args.isNoTtl()) {
			return writer;
		}
		log.info("Applying --no-ttl on binary keys (mcache id rewrite)");
		return new ProcessingItemWriter<>(new FunctionItemProcessor<>(Replicate::removeTtl), writer);
	}

	private static KeyValue<byte[]> removeTtl(KeyValue<byte[]> item) {
		item.setTtl(0);
		return item;
	}

	protected ItemWriter<KeyValue<byte[]>> replicateWriter() {
		RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer = writer();
		configureTargetRedisWriter(writer);
		return processingWriter(writer);
	}

	private boolean shouldCompare() {
		return !noReplace && !ignoreExpired && !isMcacheIdRewrite() && compareMode != CompareMode.NONE && !getJobArgs().isDryRun();
	}

	private boolean isMcacheIdRewrite() {
		return sourceMcacheId != null || targetMcacheId != null;
	}

	@Override
	protected void teardown() {
		if (noReplaceFilter != null) {
			noReplaceFilter.close();
			noReplaceFilter = null;
		}
		super.teardown();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected RedisItemReader<byte[], byte[]> reader() {
		if (isStruct()) {
			log.info("Creating Redis data-structure reader");
			return RedisItemReader.struct(ByteArrayCodec.INSTANCE);
		}
		log.info("Creating Redis dump reader");
		return (RedisItemReader) RedisItemReader.dump();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RedisItemWriter<byte[], byte[], KeyValue<byte[]>> writer() {
		if (isStruct()) {
			log.info("Creating Redis data-structure writer");
			return RedisItemWriter.struct(ByteArrayCodec.INSTANCE);
		}
		log.info("Creating Redis dump writer");
		return (RedisItemWriter) RedisItemWriter.dump();
	}

	@Override
	protected boolean isStruct() {
		return type == Type.STRUCT;
	}

	private String taskName(RedisItemReader<?, ?> reader) {
		switch (reader.getMode()) {
		case SCAN:
			return SCAN_TASK_NAME;
		case LIVEONLY:
			return LIVEONLY_TASK_NAME;
		default:
			return LIVE_TASK_NAME;
		}
	}

	public RedisWriterArgs getTargetRedisWriterArgs() {
		return targetRedisWriterArgs;
	}

	public void setTargetRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.targetRedisWriterArgs = redisWriterArgs;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public boolean isLogKeys() {
		return logKeys;
	}

	public void setLogKeys(boolean enable) {
		this.logKeys = enable;
	}

	public boolean isNoReplace() {
		return noReplace;
	}

	public void setNoReplace(boolean noReplace) {
		this.noReplace = noReplace;
	}

	public boolean isIgnoreExpired() {
		return ignoreExpired;
	}

	public void setIgnoreExpired(boolean ignoreExpired) {
		this.ignoreExpired = ignoreExpired;
	}

	public boolean isMcache() {
		return mcache;
	}

	public void setMcache(boolean mcache) {
		this.mcache = mcache;
	}

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	public boolean isAlreadyHasPrefix() {
		return alreadyHasPrefix;
	}

	public void setAlreadyHasPrefix(boolean alreadyHasPrefix) {
		this.alreadyHasPrefix = alreadyHasPrefix;
	}

	public Integer getSourceMcacheId() {
		return sourceMcacheId;
	}

	public void setSourceMcacheId(Integer sourceMcacheId) {
		this.sourceMcacheId = sourceMcacheId;
	}

	public Integer getTargetMcacheId() {
		return targetMcacheId;
	}

	public void setTargetMcacheId(Integer targetMcacheId) {
		this.targetMcacheId = targetMcacheId;
	}

	public CompareMode getCompareMode() {
		return compareMode;
	}

	public void setCompareMode(CompareMode compareMode) {
		this.compareMode = compareMode;
	}

}

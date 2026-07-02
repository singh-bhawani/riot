package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.RedisCodec;

/**
 * Drops key-value records whose key is already present in the target, so that
 * existing target keys are left untouched instead of being overwritten. Enabled
 * via {@code --no-replace}.
 * <p>
 * Existence is checked per key at processing time via {@code EXISTS} on a
 * dedicated target connection; this is best-effort and NOT atomic against
 * concurrent writes to the target (small TOCTOU window between the check and the
 * subsequent write). It is intended for bulk seeding into a target that is not
 * taking concurrent writes.
 */
public class NoReplaceFilter<K, V> implements ItemProcessor<KeyValue<K>, KeyValue<K>>, AutoCloseable {

	private final StatefulConnection<K, V> connection;
	private final RedisKeyCommands<K, V> commands;
	private final Function<K, String> keyToString;
	private final Logger log;

	public NoReplaceFilter(AbstractRedisClient client, boolean cluster, RedisCodec<K, V> codec, Logger log) {
		if (cluster) {
			StatefulRedisClusterConnection<K, V> conn = ((RedisClusterClient) client).connect(codec);
			this.connection = conn;
			this.commands = conn.sync();
		} else {
			StatefulRedisConnection<K, V> conn = ((RedisClient) client).connect(codec);
			this.connection = conn;
			this.commands = conn.sync();
		}
		this.keyToString = BatchUtils.toStringKeyFunction(codec);
		this.log = log;
	}

	@Override
	public KeyValue<K> process(KeyValue<K> item) {
		K key = item.getKey();
		Long count = commands.exists(key);
		if (count != null && count > 0) {
			if (log.isInfoEnabled()) {
				log.info("Skipping key already present in target: {}", keyToString.apply(key));
			}
			return null;
		}
		return item;
	}

	@Override
	public void close() {
		if (connection != null) {
			connection.close();
		}
	}

}

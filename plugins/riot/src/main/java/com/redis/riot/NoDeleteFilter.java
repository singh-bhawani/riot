package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

/**
 * Drops key-value records for keys that no longer exist on the source
 * (expirations and deletions surface as records for which
 * {@link KeyValue#exists(KeyValue)} is {@code false}). This prevents those
 * deletions from being propagated to the target. Enabled via
 * {@code --no-delete}.
 * <p>
 * Intended for long-running syncs where the target has become authoritative
 * (e.g. after a traffic cutover) and must not lose live keys when stale keys
 * expire on the source.
 */
public class NoDeleteFilter<K, T extends KeyValue<K>> implements ItemProcessor<T, T> {

	private final Function<K, String> keyToString;
	private final Logger log;

	public NoDeleteFilter(RedisCodec<K, ?> codec, Logger log) {
		this.keyToString = BatchUtils.toStringKeyFunction(codec);
		this.log = log;
	}

	@Override
	public T process(T item) {
		if (!KeyValue.exists(item)) {
			if (log.isInfoEnabled()) {
				log.info("Skipping delete of key not present on source: {}", keyToString.apply(item.getKey()));
			}
			return null;
		}
		return item;
	}

}

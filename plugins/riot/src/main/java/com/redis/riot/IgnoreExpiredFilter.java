package com.redis.riot;

import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

/**
 * Drops records representing a key that expired on the source (keyspace event
 * {@code expired}) so server-side TTL expirations are not propagated as deletes
 * to the target. Explicit deletions ({@code del} / {@code unlink}) are left
 * untouched and still propagate. Enabled via {@code --ignore-expired}.
 * <p>
 * Intended for long-running syncs where the target has become authoritative
 * (e.g. after a traffic cutover): keys merely expiring on the stale source must
 * not delete live keys on the target, while genuine deletions should still
 * replicate.
 */
public class IgnoreExpiredFilter<K, T extends KeyValue<K>> implements ItemProcessor<T, T> {

	public static final String EXPIRED_EVENT = "expired";

	private final Function<K, String> keyToString;
	private final Logger log;

	public IgnoreExpiredFilter(RedisCodec<K, ?> codec, Logger log) {
		this.keyToString = BatchUtils.toStringKeyFunction(codec);
		this.log = log;
	}

	@Override
	public T process(T item) {
		if (!KeyValue.exists(item) && EXPIRED_EVENT.equals(item.getEvent())) {
			if (log.isInfoEnabled()) {
				log.info("Skipping expired key (not propagating expiration to target): {}",
						keyToString.apply(item.getKey()));
			}
			return null;
		}
		return item;
	}

}

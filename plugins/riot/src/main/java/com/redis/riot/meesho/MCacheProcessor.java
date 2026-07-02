package com.redis.riot.meesho;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.RedisCodec;

/**
 * MCache key/value transformation applied during replication:
 * <ul>
 * <li>prepends {@code keyPrefix} to every key;</li>
 * <li>for string values, prepends an MCache marker byte ({@code 0x01});</li>
 * <li>when {@code alreadyHasPrefix} is set, strips a leading marker byte from string values before re-adding it.</li>
 * </ul>
 * Forward-ported from the {@code mcache_processor} branch to the current
 * {@code KeyValue<K>} API (previously {@code KeyValue<K, Object>}).
 */
public class MCacheProcessor<K, T extends KeyValue<K>> implements ItemProcessor<T, T> {

	private final Function<K, String> keyToString;
	private final Logger log;
	private final RedisCodec<K, ?> codec;
	private final String keyPrefix;
	private final boolean alreadyHasPrefix;

	public MCacheProcessor(RedisCodec<K, ?> codec, Logger log, String keyPrefix, boolean alreadyHasPrefix) {
		this.keyPrefix = keyPrefix;
		this.alreadyHasPrefix = alreadyHasPrefix;
		this.codec = codec;
		this.keyToString = BatchUtils.toStringKeyFunction(codec);
		this.log = log;
	}

	@Override
	@SuppressWarnings("unchecked")
	public T process(T item) throws Exception {
		K key = item.getKey();
		String modifiedKey = keyPrefix + string(key);
		key = (K) modifiedKey.getBytes(StandardCharsets.UTF_8);
		item.setKey(key);

		if (alreadyHasPrefix && KeyValue.TYPE_STRING.equals(item.getType()) && item.getValue() instanceof byte[]) {
			byte[] originalValue = (byte[]) item.getValue();
			if (originalValue.length > 1) {
				byte[] newValue = new byte[originalValue.length - 1];
				System.arraycopy(originalValue, 1, newValue, 0, newValue.length);
				item.setValue(newValue);
			} else {
				throw new IllegalArgumentException("Array must contain more than one byte.");
			}
		}

		if (KeyValue.TYPE_STRING.equals(item.getType()) && item.getValue() instanceof byte[]) {
			byte[] originalValue = (byte[]) item.getValue();
			byte prefixByte = 0x01;
			byte[] newValue = new byte[originalValue.length + 1];
			newValue[0] = prefixByte;
			System.arraycopy(originalValue, 0, newValue, 1, originalValue.length);
			item.setValue(newValue);
		}

		return item;
	}

	private String string(K key) {
		return keyToString.apply(key);
	}
}

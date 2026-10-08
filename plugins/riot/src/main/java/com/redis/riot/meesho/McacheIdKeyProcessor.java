package com.redis.riot.meesho;

import java.util.Arrays;

import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.item.redis.common.KeyValue;

/**
 * Rewrites the binary mcacheId key prefix used by Meesho's memcoil client when
 * {@code keyPrefixEnabled} is set. memcoil stores every key as a 2-byte
 * big-endian mcacheId followed by the original key bytes (e.g. mcacheId
 * {@code 82} &rarr; {@code [0x00, 0x52] + "user:1001"}).
 * <ul>
 * <li>{@code sourceId} set: the 2-byte prefix for that id is stripped from
 * every key. Keys that do not start with exactly that prefix, or that consist
 * only of the prefix (empty remainder), are skipped (not replicated).</li>
 * <li>{@code targetId} set: the 2-byte prefix for that id is prepended to every
 * key.</li>
 * <li>Both set: keys are moved from one mcacheId to another (e.g. 82 &rarr;
 * 195).</li>
 * </ul>
 * Values are never modified. Delete events flow through the same processor, so
 * a deletion of {@code 82:key} on the source is applied to {@code 195:key} on
 * the target. Enabled via {@code --source-mcache-id} /
 * {@code --target-mcache-id}.
 */
public class McacheIdKeyProcessor<T extends KeyValue<byte[]>> implements ItemProcessor<T, T> {

	public static final int PREFIX_LENGTH = 2;
	public static final int MIN_ID = 0;
	public static final int MAX_ID = 0xFFFF;

	private final byte[] sourcePrefix;
	private final byte[] targetPrefix;
	private final Logger log;

	/**
	 * @param sourceId mcacheId to strip from source keys, or {@code null} to leave
	 *                 source keys as-is
	 * @param targetId mcacheId to prepend to target keys, or {@code null} to add
	 *                 no prefix
	 * @param log      logger used for skipped keys (debug level)
	 * @throws IllegalArgumentException if both ids are null or an id is outside
	 *                                  {@code 0..65535}
	 */
	public McacheIdKeyProcessor(Integer sourceId, Integer targetId, Logger log) {
		if (sourceId == null && targetId == null) {
			throw new IllegalArgumentException("At least one of source or target mcache id must be set");
		}
		this.sourcePrefix = sourceId == null ? null : prefix(sourceId);
		this.targetPrefix = targetId == null ? null : prefix(targetId);
		this.log = log;
	}

	/**
	 * Encodes an mcacheId as the 2-byte big-endian key prefix used by memcoil.
	 *
	 * @throws IllegalArgumentException if the id is outside {@code 0..65535}
	 */
	public static byte[] prefix(int id) {
		validateId(id);
		return new byte[] { (byte) (id >>> 8), (byte) id };
	}

	/**
	 * @throws IllegalArgumentException if the id is null or outside
	 *                                  {@code 0..65535}
	 */
	public static void validateId(Integer id) {
		if (id == null || id < MIN_ID || id > MAX_ID) {
			throw new IllegalArgumentException(
					String.format("Invalid mcache id %s: must be in range %d..%d", id, MIN_ID, MAX_ID));
		}
	}

	@Override
	public T process(T item) {
		byte[] key = item.getKey();
		if (key == null) {
			return item;
		}
		if (sourcePrefix != null) {
			if (!hasPrefix(key, sourcePrefix)) {
				if (log.isDebugEnabled()) {
					log.debug("Skipping key without expected mcache id prefix {}: {}", toDisplay(sourcePrefix),
							toDisplay(key));
				}
				return null;
			}
			key = Arrays.copyOfRange(key, sourcePrefix.length, key.length);
		}
		if (targetPrefix != null) {
			byte[] prefixed = new byte[targetPrefix.length + key.length];
			System.arraycopy(targetPrefix, 0, prefixed, 0, targetPrefix.length);
			System.arraycopy(key, 0, prefixed, targetPrefix.length, key.length);
			key = prefixed;
		}
		item.setKey(key);
		return item;
	}

	/**
	 * True if {@code key} starts with {@code prefix} and has a non-empty
	 * remainder.
	 */
	private static boolean hasPrefix(byte[] key, byte[] prefix) {
		if (key.length <= prefix.length) {
			return false;
		}
		for (int i = 0; i < prefix.length; i++) {
			if (key[i] != prefix[i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Renders bytes for logging: printable ASCII as-is, everything else as
	 * {@code \xHH}, so binary prefixes stay readable and log-safe.
	 */
	static String toDisplay(byte[] bytes) {
		StringBuilder sb = new StringBuilder(bytes.length + 8);
		for (byte b : bytes) {
			int c = b & 0xFF;
			if (c >= 0x20 && c < 0x7F && c != '\\') {
				sb.append((char) c);
			} else {
				sb.append(String.format("\\x%02X", c));
			}
		}
		return sb.toString();
	}

}

package com.redis.riot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.StringCodec;

class IgnoreExpiredFilterTests {

	private final IgnoreExpiredFilter<String, KeyValue<String>> filter = new IgnoreExpiredFilter<>(StringCodec.UTF8,
			LoggerFactory.getLogger(IgnoreExpiredFilterTests.class));

	private static KeyValue<String> tombstone(String key, String event) {
		KeyValue<String> kv = new KeyValue<>();
		kv.setKey(key);
		kv.setType(KeyValue.TYPE_NONE);
		kv.setTtl(KeyValue.TTL_NO_KEY);
		kv.setEvent(event);
		return kv;
	}

	@Test
	void dropsExpiredKey() {
		// Server-side TTL expiration must not be propagated to the target.
		Assertions.assertNull(filter.process(tombstone("k1", IgnoreExpiredFilter.EXPIRED_EVENT)),
				"expired source key should be dropped");
	}

	@Test
	void keepsDeletedKey() {
		// Explicit deletions should still propagate to the target.
		KeyValue<String> del = tombstone("k2", "del");
		Assertions.assertSame(del, filter.process(del), "deleted source key should still propagate");
		KeyValue<String> unlink = tombstone("k3", "unlink");
		Assertions.assertSame(unlink, filter.process(unlink), "unlinked source key should still propagate");
	}

	@Test
	void keepsExistingKey() {
		KeyValue<String> live = new KeyValue<>();
		live.setKey("k4");
		live.setType(KeyValue.TYPE_STRING);
		live.setTtl(KeyValue.TTL_NONE);
		live.setEvent("set");
		Assertions.assertSame(live, filter.process(live), "existing source key should be replicated normally");
	}

}

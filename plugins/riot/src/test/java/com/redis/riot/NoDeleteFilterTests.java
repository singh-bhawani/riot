package com.redis.riot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.codec.StringCodec;

class NoDeleteFilterTests {

	private final NoDeleteFilter<String, KeyValue<String>> filter = new NoDeleteFilter<>(StringCodec.UTF8,
			LoggerFactory.getLogger(NoDeleteFilterTests.class));

	@Test
	void dropsDeletedSourceKey() {
		// A key that was expired/deleted on the source surfaces as a record with
		// type=none and ttl=-2 (KeyValue.exists == false).
		KeyValue<String> tombstone = new KeyValue<>("k1");
		tombstone.setType(KeyValue.TYPE_NONE);
		tombstone.setTtl(KeyValue.TTL_NO_KEY);
		Assertions.assertNull(filter.process(tombstone), "deleted/expired source key should be dropped, not propagated");
	}

	@Test
	void keepsExistingKey() {
		KeyValue<String> live = new KeyValue<>("k2");
		live.setType(KeyValue.TYPE_STRING);
		live.setTtl(KeyValue.TTL_NONE);
		Assertions.assertSame(live, filter.process(live), "existing source key should be replicated normally");
	}

}

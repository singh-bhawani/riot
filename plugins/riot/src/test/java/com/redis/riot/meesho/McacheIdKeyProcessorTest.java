package com.redis.riot.meesho;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.item.redis.common.KeyValue;

class McacheIdKeyProcessorTest {

	private static final Logger log = LoggerFactory.getLogger(McacheIdKeyProcessorTest.class);

	private static byte[] bytes(String s) {
		return s.getBytes(StandardCharsets.UTF_8);
	}

	private static byte[] concat(byte[] a, byte[] b) {
		byte[] out = new byte[a.length + b.length];
		System.arraycopy(a, 0, out, 0, a.length);
		System.arraycopy(b, 0, out, a.length, b.length);
		return out;
	}

	private static KeyValue<byte[]> kv(byte[] key, byte[] value) {
		KeyValue<byte[]> kv = new KeyValue<>();
		kv.setKey(key);
		kv.setType(KeyValue.TYPE_STRING);
		kv.setValue(value);
		return kv;
	}

	private static McacheIdKeyProcessor<KeyValue<byte[]>> processor(Integer source, Integer target) {
		return new McacheIdKeyProcessor<>(source, target, log);
	}

	@Test
	void prefixEncoding() {
		Assertions.assertArrayEquals(new byte[] { 0x00, 0x08 }, McacheIdKeyProcessor.prefix(8));
		Assertions.assertArrayEquals(new byte[] { 0x00, 0x52 }, McacheIdKeyProcessor.prefix(82));
		Assertions.assertArrayEquals(new byte[] { 0x00, (byte) 0xC3 }, McacheIdKeyProcessor.prefix(195));
		Assertions.assertArrayEquals(new byte[] { 0x01, 0x00 }, McacheIdKeyProcessor.prefix(256));
		Assertions.assertArrayEquals(new byte[] { 0x00, 0x00 }, McacheIdKeyProcessor.prefix(0));
		Assertions.assertArrayEquals(new byte[] { (byte) 0xFF, (byte) 0xFF }, McacheIdKeyProcessor.prefix(65535));
	}

	@Test
	void swapsSourceToTarget() {
		byte[] value = bytes("v1");
		KeyValue<byte[]> item = kv(concat(new byte[] { 0x00, 0x52 }, bytes("user:1001")), value);
		KeyValue<byte[]> out = processor(82, 195).process(item);
		Assertions.assertNotNull(out);
		Assertions.assertArrayEquals(concat(new byte[] { 0x00, (byte) 0xC3 }, bytes("user:1001")), out.getKey());
		Assertions.assertSame(value, out.getValue());
		Assertions.assertArrayEquals(bytes("v1"), (byte[]) out.getValue());
	}

	@Test
	void stripsSourceOnly() {
		KeyValue<byte[]> item = kv(concat(new byte[] { 0x00, 0x52 }, bytes("user:1001")), bytes("v1"));
		KeyValue<byte[]> out = processor(82, null).process(item);
		Assertions.assertNotNull(out);
		Assertions.assertArrayEquals(bytes("user:1001"), out.getKey());
		Assertions.assertArrayEquals(bytes("v1"), (byte[]) out.getValue());
	}

	@Test
	void addsTargetOnly() {
		KeyValue<byte[]> item = kv(bytes("user:1001"), bytes("v1"));
		KeyValue<byte[]> out = processor(null, 195).process(item);
		Assertions.assertNotNull(out);
		Assertions.assertArrayEquals(concat(new byte[] { 0x00, (byte) 0xC3 }, bytes("user:1001")), out.getKey());
		Assertions.assertArrayEquals(bytes("v1"), (byte[]) out.getValue());
	}

	@Test
	void skipsNonMatchingPrefix() {
		McacheIdKeyProcessor<KeyValue<byte[]>> p = processor(82, 195);
		Assertions.assertNull(p.process(kv(concat(new byte[] { 0x00, 0x08 }, bytes("k2")), bytes("v"))));
		// Second byte matches but first does not.
		Assertions.assertNull(p.process(kv(concat(new byte[] { 0x01, 0x52 }, bytes("k2")), bytes("v"))));
		// Plain text key without binary prefix.
		Assertions.assertNull(p.process(kv(bytes("user:1001"), bytes("v"))));
	}

	@Test
	void skipsKeyEqualToOrShorterThanPrefix() {
		McacheIdKeyProcessor<KeyValue<byte[]>> p = processor(82, null);
		Assertions.assertNull(p.process(kv(new byte[] { 0x00, 0x52 }, bytes("v"))));
		Assertions.assertNull(p.process(kv(new byte[] { 0x00 }, bytes("v"))));
		Assertions.assertNull(p.process(kv(new byte[0], bytes("v"))));
	}

	@Test
	void appliesToDeleteEvents() {
		KeyValue<byte[]> del = new KeyValue<>();
		del.setKey(concat(new byte[] { 0x00, 0x52 }, bytes("k")));
		del.setType(KeyValue.TYPE_NONE);
		del.setTtl(KeyValue.TTL_NO_KEY);
		del.setEvent("del");
		KeyValue<byte[]> out = processor(82, 195).process(del);
		Assertions.assertNotNull(out);
		Assertions.assertArrayEquals(concat(new byte[] { 0x00, (byte) 0xC3 }, bytes("k")), out.getKey());
	}

	@Test
	void validatesIdRange() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> processor(-1, null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> processor(null, 65536));
		Assertions.assertThrows(IllegalArgumentException.class, () -> McacheIdKeyProcessor.prefix(70000));
		Assertions.assertThrows(IllegalArgumentException.class, () -> processor(null, null));
		Assertions.assertDoesNotThrow(() -> processor(0, 65535));
	}

	@Test
	void displayIsHexSafe() {
		Assertions.assertEquals("\\x00\\xC3k1",
				McacheIdKeyProcessor.toDisplay(concat(new byte[] { 0x00, (byte) 0xC3 }, bytes("k1"))));
	}

}

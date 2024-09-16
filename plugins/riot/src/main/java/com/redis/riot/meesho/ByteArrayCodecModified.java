package com.redis.riot.meesho;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.ToByteBufEncoder;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class ByteArrayCodecModified implements RedisCodec<byte[], byte[]>, ToByteBufEncoder<byte[], byte[]> {

    public static final ByteArrayCodecModified INSTANCE = new ByteArrayCodecModified();

    private static final byte[] EMPTY = new byte[0];

    private static final byte[] DEFAULT = new byte[]{100, 101, 102};

    @Override
    public void encodeKey(byte[] key, ByteBuf target) {
        key = DEFAULT;

        if (key != null) {
            target.writeBytes(key);
        }
    }

    @Override
    public void encodeValue(byte[] value, ByteBuf target) {
        if (value != null) {
            target.writeBytes(value);
        }
    }

    @Override
    public int estimateSize(Object keyOrValue) {

        if (keyOrValue == null) {
            return 0;
        }

        return ((byte[]) keyOrValue).length;
    }

    @Override
    public boolean isEstimateExact() {
        return true;
    }

    @Override
    public byte[] decodeKey(ByteBuffer bytes) {
        return DEFAULT;
//        return getBytes(bytes);
    }

    @Override
    public byte[] decodeValue(ByteBuffer bytes) {
        return getBytes(bytes);
    }

    @Override
    public ByteBuffer encodeKey(byte[] key) {
        return ByteBuffer.wrap(DEFAULT);

//        if (key == null) {
//            return ByteBuffer.wrap(EMPTY);
//        }
//
//        return ByteBuffer.wrap(key);
    }

    @Override
    public ByteBuffer encodeValue(byte[] value) {
        if (value == null) {
            return ByteBuffer.wrap(EMPTY);
        }

        return ByteBuffer.wrap(value);
    }

    private static byte[] getBytes(ByteBuffer buffer) {

        int remaining = buffer.remaining();

        if (remaining == 0) {
            return EMPTY;
        }

        byte[] b = new byte[remaining];
        buffer.get(b);
        return b;
    }
}

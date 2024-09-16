package com.redis.riot.meesho;

import com.redis.spring.batch.item.redis.common.BatchUtils;
import com.redis.spring.batch.item.redis.common.KeyValue;
import io.lettuce.core.codec.RedisCodec;
import org.slf4j.Logger;
import org.springframework.batch.item.ItemProcessor;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public class MCacheProcessor <K, T extends KeyValue<K, Object>> implements ItemProcessor<T, T> {
    private final Function<K, String> keyToString;
    private final Logger log;
    RedisCodec<K, ?> codec;

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
    public T process(T item) throws Exception {

        K key = item.getKey();
        String modifiedKey = keyPrefix + string(key);
        key = (K) modifiedKey.getBytes(StandardCharsets.UTF_8);
        item.setKey(key);


        if(alreadyHasPrefix && "string".equals(item.getType()) && item.getValue() instanceof byte[])
        {
            byte[] originalValue = (byte[]) item.getValue();
            log.info("First byte is : {}", originalValue[0]);
            if (originalValue.length > 1) {
                byte[] newValue = new byte[originalValue.length - 1];
                System.arraycopy(originalValue, 1, newValue, 0, newValue.length);
                item.setValue(newValue);

                log.info("New value is : {}", string((K) item.getValue()));
            } else {
                throw new IllegalArgumentException("Array must contain more than one byte.");
            }
        }


        if ("string".equals(item.getType()) && item.getValue() instanceof byte[] originalValue) {
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

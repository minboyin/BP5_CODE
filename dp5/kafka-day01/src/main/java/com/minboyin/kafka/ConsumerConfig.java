package com.minboyin.kafka;

/**
 * Created by minbo on 2021/3/25 13:12
 */

public class ConsumerConfig {
    public static final String BOOT_STRAP_CONFIG= "bootstrap.servers";
    public static final String KEY_SERIALIZER = "";
    public static final String VALUE_SERIALIZER = "";
    public static final long BUFFER_MEMORY = 33554432;
    public static final long BATCH_SIZE = 323840;
    public static final long LINGER_MS = 100;
    public static final int ACKS = -1;
    public static final int RETRIES = 3;
    public static final long MAX_BLOCK_MS = 3000;

}

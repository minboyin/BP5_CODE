package com.minboyin.kafka;

/**
 * Created by minbo on 2021/3/25 13:12
 */

public class ProducerConfig {
    public static final String BOOT_STRAP_CONFIG= "bootstrap.servers";
    public static final String KEY_DESERIALIZER = "";
    public static final String VALUE_DESERIALIZER = "";
    public static final String GROUP_ID = "";
    public static final long HEARTBEAT_INTERVAL_MS = 1000;
    public static final long SESSION_TIME_OUT = 10*1000;
    public static final long MAX_POLL_INTERVAL_MS =30*1000;
    public static final long FETCH_MAX_BYTES = 10485760;
    public static final long MAX_POLL_RECORDS = 500;
    public static final int CONNECTION_MAX_IDLE_MS = -1;
    public static final Boolean ENABLE_AUTO_COMMIT = true;
    public static final long AUTO_COMMIT_INTERVAL_MS = 1000;
    public static final String AUTO_OFFSET_RESET = "latest";

}

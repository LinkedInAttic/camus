package com.linkedin.camus.coders;

/**
 * Container for messages.  Enables the use of a custom message decoder with knowledge
 * of where these values are stored in the message schema
 * 
 * @author kgoodhop
 *
 * @param <R> The type of decoded payload
 */
public class CamusWrapper<R> {
    private R record;
    private long timestamp;
    private String server = "unknown_server";
    private String service = "unknown_service";

    public CamusWrapper(R record) {
        this.record = record;
        this.timestamp = System.currentTimeMillis();
    }
    
    public CamusWrapper(R record, long timestamp) {
        this.record = record;
        this.timestamp = timestamp;
    }
    
    public CamusWrapper(R record, long timestamp, String server, String service) {
        this.record = record;
        this.timestamp = timestamp;
        this.server = server;
        this.service = service;
    }

    /**
     * Returns the payload record for a single message
     * @return
     */
    public R getRecord() {
        return record;
    }

    /**
     * Returns current if not set by the decoder
     * @return
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the producing server of this message
     * @return
     */
    public String getServer() {
        return server;
    }

    /**
     * Returns the producing service of this message
     * @return
     */
    public String getService() {
        return service;
    }

}

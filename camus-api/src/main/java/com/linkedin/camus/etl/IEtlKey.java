package com.linkedin.camus.etl;

public interface IEtlKey {
    String getServer();

    String getService();

    long getTime();

    String getTopic();

    String getNodeId();

    int getPartition();

    long getBeginOffset();

    long getOffset();

    long getChecksum();

}

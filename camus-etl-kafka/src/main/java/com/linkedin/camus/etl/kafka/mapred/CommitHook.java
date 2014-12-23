package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.kafka.common.EtlKey;

import java.io.IOException;
import java.util.Map;

public interface CommitHook {
    public void commit(final Map<String, EtlKey> offsets) throws IOException;
}

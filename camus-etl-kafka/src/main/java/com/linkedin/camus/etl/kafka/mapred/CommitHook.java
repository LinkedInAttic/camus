package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;
/* Provide post commit behavior on completion of output
 *
 * User defined hooks can be added as a comma separated list in the config file:
 * etl.commit.hooks=com.example.AddHivePartitionsHook
 *
 */
public interface CommitHook {
    /**
     * Called once the task's temporary output is ready to be moved to final location
     *
     * @param context Context of the task whose output is being written.
     * @param offsets Current kafka offsets
     * @param workPath working path for task output
     * @throws Exception if commit is not
     */
    public void commit(final TaskAttemptContext context, final Map<String, EtlKey> offsets, Path workPath) throws Exception;
}

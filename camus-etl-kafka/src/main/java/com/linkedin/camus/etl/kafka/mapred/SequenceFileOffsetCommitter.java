package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.util.Map;

public class SequenceFileOffsetCommitter implements CommitHook {
    static final Logger log = Logger.getLogger(SequenceFileOffsetCommitter.class);

    @Override
    public void commit(final TaskAttemptContext context, final Map<String, EtlKey> offsets, Path workPath) throws Exception {
        final FileSystem fs = FileSystem.get(context.getConfiguration());
        Path p = new Path(workPath, EtlMultiOutputFormat.getUniqueFile(context, EtlMultiOutputFormat.OFFSET_PREFIX, ""));
        SequenceFile.Writer offsetWriter = SequenceFile.createWriter(fs,
                context.getConfiguration(),
                p,
                EtlKey.class, NullWritable.class);

        log.info("Writing sequence file offsets to " + p.toString());
        for (String s : offsets.keySet()) {
            offsetWriter.append(offsets.get(s), NullWritable.get());
        }

        offsetWriter.close();

        log.debug("Offsets written to sequence files");
    }
}

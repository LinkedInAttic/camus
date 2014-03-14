package com.liquidm.camus.com.liquidm.camus;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.pig.TupleWriteSupport;

import java.io.IOException;

/**
 * Created by sixtus on 14.03.14.
 */
class ParquetCamusWriter implements RecordWriterProvider {

    @Override
    public String getFilenameExtension() {
        return ".parquet";
    }


    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {
        String pigSchemaString = "TODO";

        Schema pigSchema = Utils.getSchemaFromString(pigSchemaString);
        TupleWriteSupport writeSupport = new TupleWriteSupport(pigSchema);

        Path cwd = committer.getWorkPath();
        Path file = new Path(cwd, EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

        final ParquetWriter<Tuple> writer = new ParquetWriter<Tuple>(file, writeSupport, CompressionCodecName.UNCOMPRESSED,  ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, false, false);

        return new RecordWriter<IEtlKey, CamusWrapper>() {

            @Override
            public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
                TupleFactory tf = TupleFactory.getInstance();
                Tuple t = tf.newTuple();

                // TODO write tuple

                writer.write(t);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}

package com.linkedin.camus.shopify;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class ShopifyCombineFileInputFormat extends CombineFileInputFormat<LongWritable, Text> {

    public ShopifyCombineFileInputFormat() {
        super();
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<LongWritable, Text>((CombineFileSplit) split, context, ShopifyCombineRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    public static class ShopifyCombineRecordReader extends RecordReader<LongWritable, Text> {
        private static final Log LOG = LogFactory.getLog(ShopifyCombineRecordReader.class);
        private CompressionCodecFactory compressionCodecs = null;
        private LongWritable key = new LongWritable();
        private Text value = new Text();

        private long start;
        private long pos;
        private long end;
        private LineReader in;
        private int maxLineLength;

        public ShopifyCombineRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
            Configuration job = context.getConfiguration();
            this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
            final Path path = split.getPath(index);
            compressionCodecs = new CompressionCodecFactory(job);
            final CompressionCodec codec = compressionCodecs.getCodec(path);

            // open the file and seek to the start of the split
            FileSystem fs = path.getFileSystem(job);
            FSDataInputStream fileIn = fs.open(path);
            boolean skipFirstLine = false;
            if (codec != null) {
                in = new LineReader(codec.createInputStream(fileIn), job);
                end = Long.MAX_VALUE;
            } else {
                if (start != 0) {
                    skipFirstLine = true;
                    --start;
                    fileIn.seek(start);
                }
                in = new LineReader(fileIn, job);
            }
            if (skipFirstLine) {  // skip first line and re-establish "start".
                start += in.readLine(new Text(), 0,
                        (int)Math.min((long)Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

        @Override
        public void initialize(InputSplit arg0, TaskAttemptContext arg1)
                throws IOException, InterruptedException {
            // Won't be called, use custom Constructor
            // `CFRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index)`
            // instead
        }

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }

        @Override
        public float getProgress() throws IOException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float)(end - start));
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            key.set(pos);
            value = new Text();
            int newSize = 0;
            while (pos < end) {
                newSize = in.readLine(value, maxLineLength,
                        Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                maxLineLength));
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < maxLineLength) {
                    break;
                }

                // line too long. try again
                LOG.info("Skipped line of size " + newSize + " at pos " +
                        (pos - newSize));
            }
            if (newSize == 0) {
                return false;
            } else {
                return true;
            }
        }
    }
}

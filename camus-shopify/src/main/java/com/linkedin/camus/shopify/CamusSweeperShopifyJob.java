package com.linkedin.camus.shopify;

import com.linkedin.camus.sweeper.mapreduce.CamusSweeperJob;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CamusSweeperShopifyJob extends CamusSweeperJob {
    @Override
    public void configureJob(String topic, Job job) {
        // setting up our input format and map output types

        super.configureInput(job,
                ShopifyCombineFileInputFormat.class,
                TimebasedEventMapper.class,
                LongWritable.class,
                Text.class);

        // setting up our output format and output types
        super.configureOutput(job,
                TextOutputFormat.class,
                PlainValueReducer.class,
                NullWritable.class,
                Text.class);
    }
}

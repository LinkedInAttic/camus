# Installation

Build Shopify's version of Camus using:
```bash
mvn clean package
```

This will create a shaded jar in the camus-shopify/target/ folder.

# Run (for testing purposes)

Copy the file camus-shopify-0.1.0-shopify1.jar to a Hadoop node using scp.

Then launch Camus using the following command:
```bash
java -Xms64M -Xmx256M -cp /etc/camus:/etc/hadoop/conf:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop/lib/*:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop/.//*:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop-hdfs/./:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/u/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/libexec/../../hadoop-yarn/.//*:/u/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/./:/u/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/lib/*:/u/cloudera/parcels/CDH/lib/hadoop-0.20-mapreduce/.//*:camus-shopify-0.1.0-shopify1.jar com.linkedin.camus.shopify.ShopifyCamysJob -P /u/apps/camus/shared/camus.properties
```

# Deploy
Shipit -> https://shipit.shopify.com/shopify/camus/production

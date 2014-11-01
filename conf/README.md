# Installation

Build Shopify's version of Camus using:
```bash
mvn clean package -pl camus-shopify
```

This will create a shaded jar in the camus-shopify/target/ folder. 

# Run (for testing purposes)

Copy the file camus-shopify-0.1.0-shopify1.jar to a Hadoop node using scp.  

Then launch Camus using the following command:
```bash
java -cp /u/opt/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/client-0.20/*:/u/opt/cloudera/parcels/CDH-4.4.0-1.cdh4.4.0.p0.39/lib/hadoop/client-0.20/*:camus-shopify-0.1.0-shopify1.jar com.linkedin.camus.etl.kafka.CamusJob -P camus.properties
```
 
# Deploy
Not yet figured out.
 

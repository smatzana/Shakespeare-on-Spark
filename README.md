# Process the body of Shakespeare works 
## Find some interesting stats 

 * Top Five words
 * Top Five phrases
 * Top Five Longest Words

## Scala Solution

I coded my solution using Maven and Scala. It assumes there's a local Spark cluster running. To run via maven:

```bash
% mvn install
% ${SPARK_HOME}/bin/spark-submit --class com.matzanas.shakespeareSpark --master 'local[4]' target/data-test-1.0-SNAPSHOT.jar 'src/main/resources/shakespeareWorks/*/*txt'
```



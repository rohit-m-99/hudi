#export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home
export HADOOP_CONF_DIR=/home/sivabala/conf/
/Users/sivabala/Documents/personal/projects/spark/spark-2.4.4-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-avro_2.12:2.4.4,org.apache.hbase:hbase-server:1.2.3,org.apache.hbase:hbase-client:1.2.3,org.apache.hbase:hbase-common:1.2.3 \
 --deploy-mode client \
 --num-executors 1 \
 --executor-memory 1g \
 --conf "spark.driver.extraJavaOptions=-XX:+PrintTenuringDistribution -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump-driver.hprof -Xloggc:/tmp/gclog.out" \
--conf "spark.executor.extraJavaOptions=-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintAdaptiveSizePolicy -XX:+UnlockDiagnosticVMOptions -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/hoodie-heapdump_executor-1.hprof -Xloggc:/tmp/gclog_exec.out" \
 --driver-memory 1g \
 --queue connectivity \
 --driver-class-path '/home/sivabala/conf' \
 --class org.apache.hudi.TestHashIndexScalaLocal \
 --jars /Users/sivabala/Documents/personal/projects/siva_hudi/hudi/hudi-common/target/hudi-common-0.5.2-SNAPSHOT.jar /Users/sivabala/Documents/personal/projects/siva_hudi/hudi/hudi-spark/target/hudi-spark_2.11-0.5.2-SNAPSHOT.jar \
30 3


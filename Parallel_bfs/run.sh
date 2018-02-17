
echo "-----------------"
echo "Compiling java..."
javac -d . ParallelBfs.java


echo "-----------------"
echo "Making jar file..."
jar cfm bfs_test.jar Manifest.txt bfs_graph/*.class



echo "-----------------"
echo "Starting dfs..."
$HADOOP_INSTALL/sbin/start-dfs.sh

echo "-----------------"
echo "Starting yarn..."
$HADOOP_INSTALL/sbin/start-yarn.sh

$HADOOP_INSTALL/bin/hdfs dfs -rm -r "/*"


echo "-----------------"
echo "Running hadoop..." 
$HADOOP_INSTALL/bin/hdfs dfs -copyFromLocal ~/dcs_project/inputMapReduce /
$HADOOP_INSTALL/bin/hadoop jar bfs_test.jar /inputMapReduce /outz_test

echo "-----------------"
echo "Success!"

echo "-----------------"
echo "Files in dfs:"
$HADOOP_INSTALL/bin/hdfs dfs -ls /

echo "-----------------"
echo "Output:"
$HADOOP_INSTALL/bin/hdfs dfs -cat /outz_test18/part-00000



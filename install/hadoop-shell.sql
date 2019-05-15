
hadoop验证：
使用hadoop自带的mapreduce验证：/home/hadoop/bd/hadoop-2.5.0-cdh5.3.6/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0-chd5.3.6.jar

echo "welcome to the beifeng tec" >> test.txt
hadoop dfs -put test.txt /
hadoop dfs -ls /
hadoop jar hadoop-mapreduce-examples-2.5.0-chd5.3.6.jar wordcount /test.txt output/1234

hh:50070/explorer.html
hh:8088/cluster/nodes

hadoop配置信息
hadoop_home/libexec
hadoop_home/etc/hadoop
hadoop_home/share/hadoop

hadoop-daemon.sh   namenode|datanode|journalnode|dfs 针对单台
hadoop-daemons.sh 针对多台
yarn-daemon.sh

hdfs命令
hdfs dfs -lsr /
hdfs dfs -ls -R /
hdfs dfs -mkdir -p /beifeng/hdfs/dir
hdfs dfs -put /home/hadoop/test.txt /beifeng/hdfs/dir
hdfs dfs -get /beifeng/hdfs/dir/test.txt /home/hadoop
hdfs dfs -cat /beifeng/test.txt
hdfs dfs -rm -R /beifeng
hdfs -fsck <path> 检查指定的path的hdfs的文件是否处于健康
hdfs dfsadmin -report 查看集群的基本信息
hdfs dfsadmin -safemode <enter|leave|get|wait>


hbase验证
hh:60010

hbase shell
status 'simple'|'detailed'|

hbase命名空间
create_namespace 'bigdata',{'comment'=>'this is our namespace'"
alter_namespace
describe_namespace
drop_namespace 'bigdata'
list_namespace 'b.*'
list_namespace_tables

list命令 看表
create '[namespace_name:]table_name','family_name_1',.....,'family_name_n'
删除表 disable '[namespace_name:]table_name' 之后调用 drop '[namespace_name:]table_name'
put '[namespace_name:]table_name','rowkey','family:[column]','value[,timestamp][....]

put 'users','row1','f:id','1'
t=get_table 'users'
t.put 'row2','f:id','2'
get 'users','row1'
t.get 'row1'
t.get 'row1','f:id'

scan 'users'
scan 'users',{FILTER=>"SingleColumnValueFilter('f','id',=,'binary:1') OR RowFilter(>=,'binary:row8')", COLUMNS=>['f:id','f:name'],LIMIT=>3,STARTROW=>'row1',ENDROW=>'row5'}

scan 'users',{FILTER=>"MultipleColumnPrefixFilter('i','name')"}
scan 'users',{FILTER=>"ColumnPrefixFilter('id')"}
scan 'users',{FILTER=>"RowFilter(>=,'binary:row8')"}

count 'users'
deleteall 'users','row1'
delete 'users','row1','f:id'
truncate 'users'
describe 'users'





hive基础命令：
create table t(id int);
create database if not exists bigdater comment 'this is test bigdater database';
show databases;
set hive.cli.print.current.db=true;
set hive.cli.print.header=true;
describe database bigdater;
describe database extended bigdater;
drop database bigdater;

create table ... as select
create table tablename like exist_tablename;

dfs -text /customers/data2.txt
create table customers(id int,name string, phone string) row format delimited fields terminated by ',' comment 'test' location '/customers';

create table complex_table_test(id int, name string, flag boolean, score array<int>,techmap<string,string>,other struct<phone:string,email:string>)
row format delimited fields terminated by '\;' collection items terminated by ',' map keys terminated by ':'
location 'hdfs://hh:8020/complex_table_test';








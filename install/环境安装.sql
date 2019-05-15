
一、创建用户

1, useradd hadoop
2, passwd hadoop
3, 给用户赋予sudo权限
chmod u+w /etc/sudoers
vim /etc/sudoers
    在root ALL=(ALL)ALL 下面加
    hadoop ALL=(ALL)ALL
4, chmod u-w /etc/sudoers

二、修改主机名及ssh免密登录

sudo hostname hh
vim /etc/sysconfig/network 修改主机名
/etc/hosts 添加IP
ssh-keygen -t rsa
进入.ssh目录，创建authorized_keys文件，并将id_rsa.pub内容添加到文件中去，修改文件权限为600
ssh hh验证


三、安装jdk

复制jdk压缩包到softs目录
tar -zvxf softs/jdk-7u79-linux-x64.tar.gz
sudo ln -s /home/hadoop/bd/jdk1.7.0_79 /usr/local/jdk
配置 ~/.bash_profile
    export JAVA_HOME=/usr/local/jdk
    export CLASSPATH=.:$JAVA_HOME/lib
    export PATH=$JAVA_HOME/bin:$PATH
source ~/.bash_profile

mongodb安装：


四、hadoop安装 见hadoop配置
第一次启动hadoop需要格式化namenode, hadoop namenode -format
2种启动方式 start-all.sh   或者  start-hdfs.sh start-yarn.sh


五、hbase安装-前期准备
    1、jdk
    2、ssh
    3、修改hostname和hosts   hbase通过hostname获取IP
    4、Hadoop安装
    5、生成环境集群（ntp+ulimit & nporc+hdfs的dfs.datanode.max.xcievers)
    6、hbase下载安装

六、hbase与mapreduce整合
    1、在etc/hadoop中创建hbase-site.xml软连接，在集群环境，hadoop运行mr时会通过找该文件查找具体的hbase环境
    ln -s /home/hadoop/bd/hbase-0.98.6-cdh5.3.6/conf/hbase-site.xml /home/hadoop/bd/hadoop-2.5.0-cdh5.3.6/etc/hadoop
    2、将hbase需要的jar包添加到hadoop运行环境，其中hbase需要的jar是在lib文件夹下的所有jar
    三种方式：
        1、hadoop-env.sh 增加hadoop_classpath环境变量，value为hbase的lib下的所有jar
        2、修改profile，在文件中添加hadoop_classpath
        3、复制jar到 hadoop的share/hadoop/common/lib 或者 share/hadoop/mapreduce
    3、使用hbase自带的server jar测试是否安装成功

hadoop jar hbase-server-xxx.jar rowcounter users


七、hive
    1、安装mysql
        yum install mysql
        yum install mysql-server
        yum install mysql-devel

        vim /etc/my.cnf
        [mysql]
        default-character-set=utf8
        [mysqld]
        character-set-server=utf8
        lower_case_table_names=1
    service mysqld start
    mysqladmin -u root password 123456
    delete from user where password = "";

    create user 'hive' identified by 'hive';
    mysql>grant all privileges on *.* to 'hive'@'%' with grant option;
    create database hive;
    alter database hive character set latin1;

    2、下载hive，配置hive-site.xml，将mysql-connector-java-5.1.31.jar移动到hive的lib目录，bin目录添加到path，
    hive --service metastore & 启动metastore服务 进入hive客户端验证



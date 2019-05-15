package com.beifeng.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TextCreateNewFile {

    public static void main(String[] args) throws IOException {
        test1();
    }

    private static void test1() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default", "hdfs://192.168.136.129:8020");
        FileSystem fs = FileSystem.get(conf);
        boolean created = fs.createNewFile(new Path("/beifeng/api/createNewFile.txt"));
        System.out.println(created ? "创建成功" : "创建失败");
        fs.close();
    }

    private static void testWrite() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default", "hdfs://192.168.136.129:8020");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/beifeng/api/createNewFile.txt");
        FSDataOutputStream dos = fs.append(path);
        dos.write("封神".getBytes());
        dos.close();
        fs.close();
    }
}

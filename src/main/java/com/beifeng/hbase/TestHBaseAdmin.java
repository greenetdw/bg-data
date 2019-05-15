package com.beifeng.hbase;

import com.beifeng.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class TestHBaseAdmin {

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseUtil.getHBaseConfiguration();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        try {
            testCreateTable(hBaseAdmin);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            hBaseAdmin.close();
        }
    }

    private static void testCreateTable(HBaseAdmin hBaseAdmin) throws IOException {
        TableName tableName = TableName.valueOf("users");
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("f"));
        htd.setMaxFileSize(10000L);
        hBaseAdmin.createTable(htd);
        System.out.println("创建成功");
    }

    static void testGetTableDescribe(HBaseAdmin hBaseAdmin) throws Exception {
//        hBaseAdmin.createNamespace(NamespaceDescriptor.create("dd").build());
        TableName name = TableName.valueOf("users");
        HTableDescriptor htd = hBaseAdmin.getTableDescriptor(name);
        System.out.println(htd);
    }

    static void testDeleteTable(HBaseAdmin hBaseAdmin) throws Exception {
        TableName name = TableName.valueOf("users");
        if (hBaseAdmin.tableExists(name)) {
            hBaseAdmin.disableTable(name);
            hBaseAdmin.deleteTable(name);
        }
    }
}
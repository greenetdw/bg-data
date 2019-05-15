package com.beifeng.hbase;

import com.beifeng.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestHTable {

    static byte[] family = Bytes.toBytes("f");

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseUtil.getHBaseConfiguration();
        HTable hTable = new HTable(conf, "users");
        try {
            testPut(hTable);
        } finally {
            hTable.close();
        }
    }

    static void testPut(HTable hTable) throws IOException {
        Put put = new Put(Bytes.toBytes("row1"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("1"));
        hTable.put(put);

        hTable.checkAndPut(Bytes.toBytes("row5"), Bytes.toBytes("f"),null,null,put);
    }

    static void testGet(HTable hTable) throws IOException {
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = hTable.get(get);
        byte[] id = result.getValue(family, Bytes.toBytes("id"));
        System.out.println("id:" + Bytes.toString(id));
    }

    static void testDelete(HTable hTable) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.deleteColumn(family, Bytes.toBytes("id"));
        hTable.delete(delete);
    }

    static void testScan(HTable hTable) throws Exception {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStartRow(Bytes.toBytes("row5"));

        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        byte[][] prefixes = new byte[2][];
        prefixes[0] = Bytes.toBytes("id");
        prefixes[1] = Bytes.toBytes("name");
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
        list.addFilter(multipleColumnPrefixFilter);
        scan.setFilter(list);

        ResultScanner rs = hTable.getScanner(scan);
        Iterator<Result> iter = rs.iterator();
        while (iter.hasNext()) {
            Result result = iter.next();
        }
    }

    static void printResult(Result result) {
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
            String family = Bytes.toString(entry.getKey());
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : entry.getValue().entrySet()) {
                String value = "";
                String column = Bytes.toString(columnEntry.getKey());
                if("age".equals(column)) {
                    value = "" + Bytes.toString(columnEntry.getValue().firstEntry().getValue());
                } else {
                    value = Bytes.toString(columnEntry.getValue().firstEntry().getValue());
                }
                System.out.println(family + ":" + column + ":" + value);
            }
        }
    }

    static void testUseHbaseConnectPool(Configuration conf) throws Exception {
        ExecutorService threads = Executors.newFixedThreadPool(10);
        HConnection pool = HConnectionManager.createConnection(conf, threads);
        HTableInterface hTable = pool.getTable("users");

        hTable.close();
        pool.close();
    }

}

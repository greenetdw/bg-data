package com.beifeng.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class HBaseTableDemo {

    static HashMap<String, String> transforContent2Map(String content) {
        HashMap<String, String> map = new HashMap<>();
        int i = 0;
        String key = "";
        StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (tokenizer.hasMoreTokens()) {
            if (i % 2 == 0) {
                ++i;
                //当前的值为value
                String tmp = tokenizer.nextToken();
                System.out.println("值=======" + tmp);
                map.put(key, tmp);
            } else {
                ++i;
                //当前值为key
                key = tokenizer.nextToken();
                System.out.println("键=======" + key);
            }
        }
        return map;
    }

    static class HbaseMapper extends TableMapper<Text, ProductModel> {

        private Text outPutKey = new Text();
        private ProductModel outPutValue = new ProductModel();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
            System.out.println("**********" + content);
            if (null == content) {
                System.err.println("数据格式错误");
                return;
            }
            HashMap<String, String> mp = transforContent2Map(content);
            System.out.println("start+++++++++++++++++++");
            for (Map.Entry<String, String> entry : mp.entrySet()) {
                System.out.println("key:" + entry.getKey() + ";value:" + entry.getValue());
            }
            System.out.println("end+++++++++++++++++++");
            if (mp.containsKey("p_id")) {
                outPutKey.set(mp.get("p_id"));
                if (mp.containsKey("p_name") && mp.containsKey("price")) {
                    outPutValue.setId(mp.get("p_id"));
                    outPutValue.setName(mp.get("p_name"));
                    outPutValue.setPrice(mp.get("price"));

                } else {
                    System.err.println("数据格式错误");
                    return;
                }
            } else {
                System.err.println("数据格式错误");
                return;
            }

            context.write(outPutKey, outPutValue);
        }
    }

    //输出的key一般为ImmutableBytesWritable
    static class HbaseReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text key, Iterable<ProductModel> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("---------" + key.toString() + "作为rowkey");
            for (ProductModel value : values) {
                ImmutableBytesWritable outputKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
                //Bytes.toBytes(key.toString()) 作为rowkey
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
                context.write(outputKey, put);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://192.168.207.10"); // hadoop的环境
        conf.set("hbase.zookeeper.quorum", "192.168.207.10"); // hbase zk环境信息

        Job job = Job.getInstance(conf, "demo");
        job.setJarByClass(HBaseTableDemo.class);

        // 设置mapper相关，mapper从hbase输入
        // 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
        // addDependencyJars集群中，参数必须为true。
        TableMapReduceUtil.initTableMapperJob("data", new Scan(), HbaseMapper.class, Text.class, ProductModel.class, job, false);

        // 设置reducer相关，reducer往hbase输出
        // 本地环境，而且fs.defaultFS为集群模式的时候，需呀设置addDependencyJars参数为false。
        TableMapReduceUtil.initTableReducerJob("online_product", HbaseReducer.class, job, null, null, null, null, false);
        int status = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("结果" + status);
        //return job;
    }
}

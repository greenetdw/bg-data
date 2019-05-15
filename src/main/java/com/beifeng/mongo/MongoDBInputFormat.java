package com.beifeng.mongo;

import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable,V> {

    /**
     * 获取分片信息
     * @param jobContext 上下文
     * @return 输入分片
     * @throws IOException IO异常
     * @throws InterruptedException
     */
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        DB mongo = Mongo.connect(new DBAddress("127.0.0.1", "hadoop"));
        DBCollection dbCollection = mongo.getCollection("persons");
        //每2条数据一个mapper
        int chunkSize = 2;
        long size = dbCollection.count();
        long chunk = size / chunkSize;
        List<InputSplit> list = new ArrayList<InputSplit>();
        for (int i = 0; i < chunk; i++) {
            if (i+1 == chunk) {
                list.add(new MongoDBInputSplit(i * chunkSize, size + 1));
            } else {
                list.add(new MongoDBInputSplit(i * chunkSize, i * chunkSize + chunkSize));
            }
        }
        return list;
    }

    public RecordReader<LongWritable, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new MongoDBRecordReader<V>(inputSplit, taskAttemptContext);
    }

    static class NullMongoDBWritable implements MongoDBWritable {

        public void readFields(DBObject dbObject) {

        }

        public void write(DBCollection dbCollection) {

        }

        public void write(DataOutput dataOutput) throws IOException {

        }

        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    static class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable,V> {

        private MongoDBInputSplit split;
        private int index;
        private Configuration conf;
        private DBCursor dbCursor;
        private LongWritable key;
        private V value;

        public MongoDBRecordReader() {
        }

        public MongoDBRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            this.initialize(split, context);
        }

        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.split = (MongoDBInputSplit)inputSplit;
            this.conf = taskAttemptContext.getConfiguration();
            key = new LongWritable();
            Class clz = conf.getClass("mapreduce.mongo.split.value.class", NullMongoDBWritable.class);
            value = (V) ReflectionUtils.newInstance(clz, conf);
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.dbCursor == null) {
                DB mongo = Mongo.connect(new DBAddress("127.0.0.1", "hadoop"));
                DBCollection dbCollection = mongo.getCollection("persons");
                dbCursor = dbCollection.find().skip((int) this.split.start).limit((int) this.split.getLength());
            }

            boolean hsNext = dbCursor.hasNext();
            if (hsNext) {
                DBObject dbObject = this.dbCursor.curr();
                this.key.set(this.split.start + index);
                this.index++;
                this.value.readFields(dbObject);

            }
            return hsNext;
        }

        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return this.key;
        }

        public V getCurrentValue() throws IOException, InterruptedException {
            return this.value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        public void close() throws IOException {
            this.dbCursor.close();
        }
    }

    static class MongoDBInputSplit extends InputSplit implements Writable {

        private long start;//[start,end)
        private long end;

        public MongoDBInputSplit() {

        }

        MongoDBInputSplit(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getLength() throws IOException, InterruptedException {
            return end - start;
        }

        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }


        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(this.start);
            dataOutput.writeLong(this.end);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.start = dataInput.readLong();
            this.end = dataInput.readLong();
        }
    }
}

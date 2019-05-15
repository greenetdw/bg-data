package com.beifeng.mongo;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.UnknownHostException;

public class MongoDBOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable,V> {


    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MongoDBRecordWriter<V>(taskAttemptContext);
    }

    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }

    static class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable, V> {

        private DBCollection dbCollection = null;

        public MongoDBRecordWriter() {

        }

        MongoDBRecordWriter(TaskAttemptContext context) throws IOException {
            DB db = Mongo.connect(new DBAddress("127.0.0,1", "hadoop"));
            dbCollection = db.getCollection("result");
        }

        public void write(NullWritable nullWritable, V value) throws IOException, InterruptedException {
            value.write(this.dbCollection);
        }

        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }
}

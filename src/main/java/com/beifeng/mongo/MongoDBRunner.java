package com.beifeng.mongo;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MongoDBRunner {

    static class PersonMongoDBWritable implements MongoDBWritable {

        private String name;
        private Integer age;
        private String sex = "";
        private Integer count = 1;

        public void readFields(DBObject dbObject) {
            this.name = dbObject.get("name").toString();
            this.sex = dbObject.get("sex").toString();
            if(dbObject.get("age") != null) {
                this.age = Integer.valueOf(dbObject.get("age").toString());
            } else  {
                this.age = null;
            }
        }

        public void write(DBCollection dbCollection) {
            DBObject dbObject = BasicDBObjectBuilder.start().add("age", this.age).add("count", this.count).get();
            dbCollection.insert(dbObject);
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.name);
            dataOutput.writeUTF(this.sex);
            if(this.age == null) {
                dataOutput.writeBoolean(false);
            } else {
                dataOutput.writeBoolean(true);
                dataOutput.writeInt(this.age);
            }
            dataOutput.writeInt(this.count);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.name = dataInput.readUTF();
            this.sex = dataInput.readUTF();
            if(dataInput.readBoolean()) {
                this.age = dataInput.readInt();
            } else {
                this.age = null;
            }
            this.count = dataInput.readInt();
        }
    }

    static class MongoDBMapper extends Mapper<LongWritable, PersonMongoDBWritable, IntWritable, PersonMongoDBWritable> {
        @Override
        protected void map(LongWritable key, PersonMongoDBWritable value, Context context) throws IOException, InterruptedException {
            if (value.age == null) {
                System.out.println("过滤数据" + value.name);
                return;
            }
            context.write(new IntWritable(value.age), value);
        }
    }

    static class MongoDBReducer extends Reducer<IntWritable, PersonMongoDBWritable, NullWritable, PersonMongoDBWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<PersonMongoDBWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(PersonMongoDBWritable value : values) {
                sum += value.count;
            }
            PersonMongoDBWritable personMongoDBWritable = new PersonMongoDBWritable();
            personMongoDBWritable.age = key.get();
            personMongoDBWritable.count = sum;
            context.write(NullWritable.get(), personMongoDBWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "自定义输入输出");
        job.setJarByClass(MongoDBRunner.class);
        job.setMapperClass(MongoDBMapper.class);
        job.setReducerClass(MongoDBReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonMongoDBWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PersonMongoDBWritable.class);
        job.setInputFormatClass(MongoDBInputFormat.class);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        job.waitForCompletion(true);
    }
}

package com.beifeng.mongo;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hadoop.io.Writable;

public interface MongoDBWritable extends Writable {

    /**
     * 从mongo中读取数据
     * @param dbObject mongodb数据库实例
     */
    void readFields(DBObject dbObject);

    void write(DBCollection dbCollection);
}

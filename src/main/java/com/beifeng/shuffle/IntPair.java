package com.beifeng.shuffle;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int compareTo(IntPair o) {
        if(o == this) {
            return 0;
        }
        int tmp = Integer.compare(this.first, o.first);
        if(tmp != 0) {
            return tmp;
        }
        tmp = Integer.compare(this.second, o.second);
        return tmp;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first);
        dataOutput.writeInt(this.second);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readInt();
        this.second = dataInput.readInt();
    }


}

package pl.edu.wat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class FloatArrayWritable extends ArrayWritable {
    FloatArrayWritable() {
        super(FloatWritable.class);
    }

    FloatArrayWritable(Writable[] writables) {
        super(FloatWritable.class);
        this.set(writables);
    }

    FloatWritable get(int index) {
        return (FloatWritable) this.get()[index];
    }

    float getFloat(int index) {
        return this.get(index).get();
    }

    @Override
    public String toString() {
        return getFloat(0) + "\t" + getFloat(1);
    }
}
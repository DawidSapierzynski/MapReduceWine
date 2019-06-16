package pl.edu.wat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WineReducer extends Reducer<FloatWritable, WineArrayWritable, Text, FloatWritable> {
    FloatWritable points = new FloatWritable();

    @Override
    protected void reduce(FloatWritable key, Iterable<WineArrayWritable> values, Context context) throws IOException, InterruptedException {

        for (WineArrayWritable wine : values) {
            this.points.set(Float.parseFloat(wine.getString(1)));

            if (points.get() > key.get()) {
                context.write(wine.get(0), points);
            }
        }

    }
}
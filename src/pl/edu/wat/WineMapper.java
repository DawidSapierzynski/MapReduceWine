package pl.edu.wat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WineMapper extends Mapper<Object, Text, FloatWritable, WineArrayWritable> {
    private static final String delimiter = "\t";
    private final FloatWritable median = new FloatWritable();
    private final Text name = new Text();
    private final Text points = new Text();
    private final Writable[] table = new Writable[]{name, points};
    private final WineArrayWritable array = new WineArrayWritable();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, FloatWritable, WineArrayWritable>.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(delimiter);
        if (values.length != 3) {
            return;
        }

        this.name.set(values[0]);
        this.points.set(values[1]);

        try {
            this.median.set(Float.parseFloat(values[2]));
        } catch (NumberFormatException e) {
            return;
        }

        this.array.set(table);
        context.write(this.median, this.array);
    }
}

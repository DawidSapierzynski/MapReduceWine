package pl.edu.wat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MedianMapper extends Mapper<Object, Text, Text, WineArrayWritable> {
    private static final String delimiter = ";";
    private static final int charY = 'C';
    private final Text designation = new Text();
    private final Text name = new Text();
    private final Text points = new Text();
    private final Writable[] table = new Writable[]{name, points};
    private final WineArrayWritable array = new WineArrayWritable();

    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, WineArrayWritable>.Context context) throws IOException, InterruptedException {
        if (charY == value.charAt(0)) {
            return;
        }

        String[] values = value.toString().split(delimiter);
        if (values.length != 14) {
            return;
        }
        this.designation.set(values[3]);
        this.name.set(values[11]);
        this.points.set(values[4]);

        this.array.set(table);
        context.write(this.designation, this.array);
    }
}
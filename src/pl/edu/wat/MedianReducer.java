package pl.edu.wat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MedianReducer extends Reducer<Text, WineArrayWritable, Text, FloatArrayWritable> {
    private FloatWritable median = new FloatWritable(2);
    private FloatWritable points = new FloatWritable();
    private Writable[] table = new Writable[]{points, median};
    private FloatArrayWritable array = new FloatArrayWritable();

    private static float median(Collection<WineArrayWritable> listWine) {
        float result;

        int[] wineArrayWritables = listWine.stream().mapToInt(WineArrayWritable::getPoints).toArray();
        Arrays.sort(wineArrayWritables);
        int len = wineArrayWritables.length;

        if (len % 2 != 0) {
            result = (float) wineArrayWritables[len / 2];
        } else {
            result = ((float) wineArrayWritables[len / 2] + (float) wineArrayWritables[len / 2 - 1]) / 2;
        }
        return result;
    }

    @Override
    protected void reduce(Text key, Iterable<WineArrayWritable> values, Context context) throws IOException, InterruptedException {

        Collection<WineArrayWritable> list = getCollectionFromIterable(values);

        median.set(median(list));

        for (WineArrayWritable wine : list) {
            try {
                this.points.set((float) wine.getPoints());
            } catch (NumberFormatException e) {
                return;
            }
            this.array.set(table);
            context.write(wine.get(0), array);
        }
    }

    public static <T> Collection<T>
    getCollectionFromIterable(Iterable<T> itr) {
        return StreamSupport.stream(itr.spliterator(), false)
                .collect(Collectors.toList());
    }
}

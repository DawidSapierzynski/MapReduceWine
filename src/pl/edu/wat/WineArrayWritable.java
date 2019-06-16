package pl.edu.wat;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class WineArrayWritable extends ArrayWritable {
    WineArrayWritable() {
        super(Text.class);
    }

    int getPoints() {
        return Integer.parseInt(this.get(1).toString());
    }

    Text get(int index) {
        return (Text) this.get()[index];
    }

    String getString(int index) {
        return this.get(index).toString();
    }
}
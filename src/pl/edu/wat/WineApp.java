package pl.edu.wat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

import static java.lang.System.err;
import static java.lang.System.exit;

public final class WineApp extends Configured {

    private static String fiveDigit(int num) {
        StringBuilder stringBuilder = new StringBuilder();
        int numberDigit = Integer.toString(num).length();

        for (int i = 0; i < 5 - numberDigit; i++) {
            stringBuilder.append("0");
        }

        stringBuilder.append(num);
        return stringBuilder.toString();
    }

    private static void setJob2(Job job2, String[] jobArgs, int numberReduce) {
        job2.setJarByClass(WineApp.class);
        job2.setReducerClass(WineReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(WineArrayWritable.class);


        for (int i = 0; i < numberReduce; i++) {
            MultipleInputs.addInputPath(job2, new Path(jobArgs[1] + "/Temp/part-r-" + fiveDigit(i)), TextInputFormat.class, WineMapper.class);
        }
        FileOutputFormat.setOutputPath(job2, new Path(jobArgs[1] + "/Result"));
        job2.setOutputFormatClass(TextOutputFormat.class);
    }

    private static void setJob1(Job job, String[] jobArgs) throws IOException {
        job.setJarByClass(WineApp.class);
        job.setMapperClass(MedianMapper.class);
        job.setReducerClass(MedianReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatArrayWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WineArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(jobArgs[1] + "/Temp"));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] jobArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int numberReduce = Integer.parseInt(conf.get("mapreduce.job.reduces"));

        if (jobArgs.length != 2) {
            err.println("Usage: pl.edu.wat.WineApp <in> <out>");
            System.exit(2);
        }

        Job job1 = Job.getInstance(conf, "Median");
        setJob1(job1, jobArgs);
        boolean result1 = job1.waitForCompletion(true);
        if (!result1)
            exit(1);


        Job job2 = Job.getInstance(conf, "Points Higher Than Median");
        setJob2(job2, jobArgs, numberReduce);
        boolean result2 = job2.waitForCompletion(true);
        exit(result2 ? 0 : 1);
    }
}
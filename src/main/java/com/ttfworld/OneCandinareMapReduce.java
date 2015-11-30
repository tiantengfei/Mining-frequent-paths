package com.ttfworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ttf on 15-11-29.
 */
public class OneCandinareMapReduce  {

    public static class OneCandinateMapper extends Mapper<Object,Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] str = value.toString().split("~");

            for(String s : str)
                context.write(new Text(s), new IntWritable(1));

        }
    }

    public static class OneCandinateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
           int maxNum = context.getConfiguration().getInt("MAX_NUM", 6);
            for(IntWritable value : values)
                sum += 1;

            if(sum > maxNum)
                context.write(new Text(key.toString() + " " + 1), new IntWritable(sum));
        }
    }
}

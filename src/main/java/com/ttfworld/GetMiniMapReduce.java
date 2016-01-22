package com.ttfworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ttf on 15-11-29.
 */
public class GetMiniMapReduce {

    public static class GetMiniMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] array = value.toString().split("\t");
            //int num = Integer.parseInt(array[1]);

            context.write(new Text(array[0]), new IntWritable(1));


        }
    }

        public static class GetMiniReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {

                int sum = 0;
                int maxNum = context.getConfiguration().getInt("MAX_NUM", 6);
                for (IntWritable value : values)
                    sum += value.get();


                if (sum > maxNum) {
                    context.write(new Text(key.toString()), new IntWritable(sum));
                }
            }
        }

}

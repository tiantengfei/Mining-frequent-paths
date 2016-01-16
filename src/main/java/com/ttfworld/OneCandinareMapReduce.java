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

            String[] siteArray = value.toString().split("~");


            for(int i = 0; i < siteArray.length -1; i++)
                context.write(new Text(siteArray[i]), new IntWritable(1));

        }
    }

    public static class OneCandinateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
           int maxNum = context.getConfiguration().getInt("MAX_NUM", 6);
            for(IntWritable value : values)
                sum += 1;


            if(sum > maxNum) {
                context.write(new Text(key.toString()), new IntWritable(sum));
                context.getCounter("CANDINATE_NUM", "candinateNum").increment(1);
            }
        }
    }
}

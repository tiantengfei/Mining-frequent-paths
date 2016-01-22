package com.ttfworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ttf on 15-11-29.
 */
public class OneMiniMapReduce {

    public static class OneCandinateMapper extends Mapper<Object,Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] siteArray = value.toString().split("\t")[0].split("~");

            List<String> ls = Arrays.asList(siteArray);
            Set<String> set = new HashSet<>(ls);

            for(String str : set)
                context.write(new Text(str), new IntWritable(1));

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

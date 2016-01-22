package com.ttfworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */

public class DuringPathMapReduce {
    public static class DuringPathMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

           Configuration conf =  context.getConfiguration();

            String startTime = conf.get("start_time");
            String endTime = conf.get("end_time");

            String v = value.toString();
            String [] a = v.split("\t");
            String []arrs = v.split("\t")[0].split("~");

          /** if(startTime != null && endTime != null){

            if(arrs[0] != null){
            //if(conf.getInt("test", 0) == 50){
                Counter counter =  context.getCounter("nums_period_time", "nums_time");
                counter.increment(1);
            }

           **/



            if(arrs[0].compareTo(startTime) > 0 &&
                    arrs[0].compareTo(endTime) < 0){

                String s = v.split("\t")[0];
                String k = s.substring(s.indexOf("~")+ 1);
                context.write(new Text(k), new Text("no use"));
            }
        }


    }// map



    public static class DuringPathReducer extends Reducer<Text, Text, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
          Counter counter =  context.getCounter("nums_period_time", "nums_time");
            counter.increment(1);
           context.write(key, new LongWritable(counter.getValue()));




        }



        }// reduce

}

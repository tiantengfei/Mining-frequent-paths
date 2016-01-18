package com.ttfworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */
public class TwoCandinareMapReduce {

    public static class TwoCandinateMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] str = value.toString().split("\t");
            context.write(new Text("twoCanidate"), new Text(str[0]));

        }
    }

    public static class TwoCandinateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> ls = new ArrayList<>();

            for(Text text : values){

                ls.add(text.toString().split("\t")[0]);
            }

            for(int i = 0; i < ls.size(); i++){

                String str = ls.get(i);
                for(int j = 0; j < ls.size(); j ++){

                    Counter couter = context.getCounter("CANDINATE_NUM", "candinateNum");
                    couter.increment(1);
                    context.write(
                            new Text(str + "~" + ls.get(j)),
                            new Text(""+ couter.getValue()  + "_c"));
                }
            }

//            for(Text text : values)
//                for(Text t : values){
//                    String str = text.toString();
//                    context.write(new Text(str.substring(str.length() - 2)+ "~"+ t.toString()), NullWritable.get());
//                }

        }
    }
}

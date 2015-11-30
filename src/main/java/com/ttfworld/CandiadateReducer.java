package com.ttfworld;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */
public class CandiadateReducer extends Reducer<Text, Text, Text, NullWritable> {


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String  k = key.toString();
        List<Path> paths = new ArrayList<>();
       // context.write(key,NullWritable.get());
        for(Text text : values){
            paths.add(new Path(text.toString()));
        }
        List<Path> head = new ArrayList<>();
        List<Path> tail = new ArrayList<>();
        for(Path p : paths){
            if(p.getHead().equals(k))
                head.add(p);
            else
                tail.add(p);
        }

        for(Path p : tail){

            for(Path p1 : head){

                p.add(p1.getPathList().get(2));
                context.write(new Text(p.toString()), NullWritable.get());
            }
        }

     //   context.write(key, NullWritable.get());


    }
}

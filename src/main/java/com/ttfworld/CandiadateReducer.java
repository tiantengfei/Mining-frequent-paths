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


        for(Text text : values){
            paths.add(new Path(text.toString()));
        }
        List<Path> head = new ArrayList<>();
        List<Path> tail = new ArrayList<>();
        for(Path p : paths){
            if(p.getHead().equals(k))
                head.add(new Path(p.toString()));
            if(p.getTail().equals(k))
                tail.add(new Path(p.toString()));
        }

//        for(int i = 0; i < tail.size(); i++){
//
//            Path  p = tail.get(i);
//
//            for(int j = 0; j < head.size(); j++){
//
//                Path pa = new Path(p.toString());
//                p.add(head.get(j).getPathList().get(1));
//                context.write(new Text(p.toString()), NullWritable.get());
//            }
//
//        }
        for(Path p : tail){

            for(Path p1 : head){

               Path pa = new Path(p.toString());
                pa.add(p1.getPathList().get(1));
                context.write(new Text(pa.toString()), NullWritable.get());
            }
        }

       // context.write(key, NullWritable.get());


    }
}

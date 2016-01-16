package com.ttfworld;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import sun.security.util.PathList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */
public class CandiadateReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String  k = key.toString();

        List<String> paths = new ArrayList<>();


        for(Text text : values){
            paths.add(text.toString());
        }

        List<String> head = new ArrayList<>();
        List<String> tail = new ArrayList<>();

        for(String p : paths){

            if(CandidateHelper.getInsance().getHead(p).equals(k))
                head.add(p);
            if(CandidateHelper.getInsance().getTail(p).equals(k))
                tail.add(p);
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

        for(String p : tail){

            for(String p1 : head){

                String newCan = CandidateHelper.getInsance().getNewCandinate(p , p1);
                Counter counter = context.getCounter("CANDINATE_NUM", "candinateNum");
                counter.increment(1L);

                context.write(new Text(newCan), new Text(counter.getValue() + "_c"));

            }
        }

       // context.write(key, NullWritable.get());
    }

}

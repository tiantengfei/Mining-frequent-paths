package com.ttfworld;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ttf on 15-11-29.
 */
public class CandidateMapper extends Mapper<Object, Text, Text, Text>{

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String []str = value.toString().split("\t");
       // context.write(new Text(str[0]), new Text("0"));
        Path path = new Path(str[0]);
        String head = path.getHead();
        context.write(new Text(head), value);
        String tail = path.getTail();
        context.write(new Text(tail), value);
    }

}

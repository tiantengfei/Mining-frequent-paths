package com.ttfworld;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ttf on 15-11-29.
 */

/**
 * 从频繁路径中得到候选
 */
public class CandidateMapper extends Mapper<Object, Text, Text, Text>{

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String []str = value.toString().split("\t");

        String head = CandidateHelper.getInsance().getHead(str[0]);


        context.write(new Text(head), new Text(str[0]));
        String tail = CandidateHelper.getInsance().getTail(str[0]);

        if(!tail.equals(head)) {
            context.write(new Text(tail), new Text(str[0]));
        }
    }

}

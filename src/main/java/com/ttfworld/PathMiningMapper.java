package com.ttfworld;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PathMiningMapper extends Mapper<Object,Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Path path = new Path(value.toString());

        int iteraNum = context.getConfiguration().getInt("ITEAR_NUM", 6);
        for(String p : path.getKeys(iteraNum)){

            context.write(new Text(p), new Text (path.toString()));
        }

    }


}

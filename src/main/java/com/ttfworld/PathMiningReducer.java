package com.ttfworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */
public  class PathMiningReducer extends Reducer<Text, Text,Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        List<String> candidates = new ArrayList<>();
        List<String> paths = new ArrayList<>();
        int max_num = context.getConfiguration().getInt("MAX_NUM",6);
        int iterNum = context.getConfiguration().getInt("ITEAR_NUM",6);

        for(Text text : values){
            if(iscandidate(text)){
                String str = text.toString();
                candidates.add(str.substring(0,str.length() - 2));//去掉　１
                continue;
            }

            paths.add(text.toString());

        }

        for(String can : candidates){
            int sumall=0;

            for(String path : paths){

                Path p = new Path(path);
                int sum = p.getCandidateNums(iterNum, can);
                sumall+=sum;

                //if(sum > max_num)
                //context.write(new Text(can + " 1"), new IntWritable(sum));

            }
            if(sumall > max_num) {
                context.write(new Text(can + " 1"), new IntWritable(sumall));
            }

        }
    }

    public boolean iscandidate(Text text){

       int a = flag(text.toString());//最后一个数是１
        if(a == 0) return false;
        return true;

    }

    public int flag(String path){
        int a = path.charAt(path.length() - 1) - 48;

        return a;

    }
}

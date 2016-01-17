package com.ttfworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                candidates.add(str.split("\t")[0]);
                continue;
            }

            paths.add(text.toString().split("\t")[0]);

        }

        /**

        if(paths.size() > 0 && candidates.size() > 1 ) {
            context.getCounter("matchNum", "match of num").increment(1);
            context.write(new Text(key.toString() + "--"), new IntWritable(1));
        }
         **/

        for(String can : candidates){
            int sum=0;
            Pattern p = Pattern.compile(can);
            for(String path : paths){


                Matcher m = p.matcher(path);

                while(m.find()){

                    sum += 1;
                }



            }


            if(sum > 0) {
                context.write(new Text(can), new IntWritable(sum));
                context.getCounter("matchNum", "match of num").increment(1);
            }


        }


/**
        for(Text t : values){
            if(!iscandidate(t))
            context.write(new Text(key.toString() + "___" + t.toString()), new IntWritable(1));

        }
 **/

    }

    public boolean iscandidate(Text text){

       String s = text.toString();

        if(s.endsWith("_c"))
            return true;


        return false;

    }


}

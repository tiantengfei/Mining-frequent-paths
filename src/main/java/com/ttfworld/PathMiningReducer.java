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
public class PathMiningReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {



        List<String> candidates = new ArrayList<>();
        List<String> paths = new ArrayList<>();

        CandidateHelper candidateHelper = CandidateHelper.getInsance();
        for (Text text : values) {
            if (candidateHelper.iscandidate(text)) {
                String str = text.toString();
                candidates.add(str.split("\t")[0]);

            } else
                paths.add(text.toString().split("\t")[0]);

        }




        if (candidates.size() > 0 && paths.size() > 0) {

            for (String can : candidates) {
                int sum = 0;
                for (String path : paths) {
                  sum =   candidateHelper.findFrequency(new Path(path), 0,
                            new Path(can), 0);
                }

                if (sum > 0) {
                    context.write(new Text(can), new IntWritable(sum));
                    context.getCounter("matchNum", "match of num").increment(1);
                }
            }

        }
    }

}

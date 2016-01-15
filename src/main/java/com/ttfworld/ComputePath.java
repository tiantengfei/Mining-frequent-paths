package com.ttfworld;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by ttf on 15-11-29.
 */

enum MyCounter{
    PATH_NUM
}
public class ComputePath  extends Configured implements Tool{


    private long pathNum;
    private long caidinateNum;


    public int run(String[] args) throws Exception {

        String input = args[0];
       // String output = args[1];

        int MaxNum = Integer.parseInt(args[1]);
        int iteraNum = Integer.parseInt(args[2]);
        int currentItera = 0;



       Job pathJob = getPathJob(input, "newPathCount/path");

        pathJob.waitForCompletion(true);
       Counters pathJobCounters = pathJob.getCounters();
        //pathNum = pathJobCoun ters.findCounter("PATH_NUM", "num of path").getValue();
        Counter pathCounter =
                pathJobCounters.findCounter(MyCounter.PATH_NUM);
        pathNum = pathCounter.getValue();

        //pathNum = getConf().getLong("PATH_NUM", 0);
        System.out.println("path num is " + pathNum);


        /**
        Job oneCandiantejob = getOneCandidate("path", "mini1", MaxNum);

        oneCandiantejob.waitForCompletion(true);


        String candidate_in = "mini1";
        String candidate_out = null;
        while(currentItera <  iteraNum - 1 ){

            int executeItera = currentItera + 2;
            Job canidateJob = null;
            Job pathMiniJob = null;
            candidate_out = "candidate" + executeItera;
            if(currentItera == 0){
                System.out.println("twoCandidate............");

                canidateJob = getTwoCandidate(candidate_in, candidate_out);

            } else {
                System.out.println("noTwocandidate.................");
                canidateJob = getCandidate(candidate_in, candidate_out);
            }
            canidateJob.waitForCompletion(true);
            String pathmini_out = "mini" + executeItera;
            pathMiniJob = getPathMiniJob("path",candidate_out, pathmini_out,MaxNum, executeItera);

            pathMiniJob.waitForCompletion(true);
            candidate_in = pathmini_out;

            ++currentItera;


        }

**/
        return 0;
    }


    public Job getTwoCandidate(String input , String output) throws IOException{

       // getConf().setInt("ITERA_NUM", iteraNum);
        Job job = new Job(getConf(), "CanidateJob");

        job.setJarByClass(getClass());
        job.setMapperClass(TwoCandinareMapReduce.TwoCandinateMapper.class);
        job.setReducerClass(TwoCandinareMapReduce.TwoCandinateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    public Job getCandidate(String input , String output) throws IOException{

        //getConf().setInt("ITERA_NUM", iteraNum);
        Job job = new Job(getConf(), "CanidateJob");

        job.setJarByClass(getClass());
        job.setMapperClass(CandidateMapper.class);
        job.setReducerClass(CandiadateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }


    public Job getPathMiniJob(String input,String input2 , String output, int maxNum, int iteraNum) throws IOException{

        getConf().setInt("MAX_NUM", maxNum);
        getConf().setInt("ITEAR_NUM", iteraNum);
        Job job = new Job(getConf(), "PathMiniJob");

        job.setJarByClass(getClass());
        job.setMapperClass(PathMiningMapper.class);
        job.setReducerClass(PathMiningReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));



        return job;


    }
    public Job getOneCandidate(String input, String output, int maxNum)throws IOException{

        getConf().setInt("MAX_NUM", maxNum);
        Job job = new Job(getConf(), "One Candiante");

        job.setJarByClass(getClass());
        job.setMapperClass(OneCandinareMapReduce.OneCandinateMapper.class);
        job.setReducerClass(OneCandinareMapReduce.OneCandinateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));


        return job;

    }
    public Job getPathJob(String input, String output) throws IOException{

        Job job = new Job(getConf(), "Path Job");
        getConf().setLong("PATH_NUM", 0);

        job.setJarByClass(getClass());
        job.setMapperClass(PathMapper.class);
        job.setReducerClass(PathReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
       // job.setOutputKeyClass(Text.class);
       // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));



        return job;
    }

    public static void main(String[] args) throws Exception{

        System.out.println("length: " + args.length);
        int i = ToolRunner.run(new ComputePath(), args);
    }
}

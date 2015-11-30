package com.ttfworld;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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
public class ComputePath  extends Configured implements Tool{



    public int run(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        int pathNum = Integer.parseInt(args[2]);


        //IterationNumEnum.MAX_PATH_NUM.setIterationNumEnum(pathNum);


      // Job pathJob = getPathJob(input, output);

      // pathJob.waitForCompletion(true);

      //  Job oneCandiantejob = getOneCandiante(args[0], "/user/ttf/candidate", pathNum);
       // getConf().setInt("Max_NUM", pathNum);


       // int num = getConf().getInt("MAX_NUM", 5);
       // System.out.println("num:" + num);
    //    oneCandiantejob.waitForCompletion(true);


       // Job canidateJob = getCanidate(args[0], args[1],2);
       // canidateJob.waitForCompletion(true);

        //Job twoCandidateJob = getTwoCanidate(args[0],args[1], 3);
        //twoCandidateJob.waitForCompletion(true);

        Job pathMiniJob = getPathMiniJob(args[0],args[1],pathNum);
        pathMiniJob.waitForCompletion(true);
        return 0;
    }


    public Job getTwoCanidate(String input , String output, int iteraNum) throws IOException{

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

    public Job getCanidate(String input , String output, int iteraNum) throws IOException{

        getConf().setInt("ITERA_NUM", iteraNum);
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


    public Job getPathMiniJob(String input , String output, int maxNum) throws IOException{

        getConf().setInt("MAX_NUM", maxNum);
        getConf().setInt("ITEAR_NUM", 2);
        Job job = new Job(getConf(), "PathMiniJob");


        job.setJarByClass(getClass());
        job.setMapperClass(PathMiningMapper.class);
        job.setReducerClass(PathMiningReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileInputFormat.addInputPath(job, new Path("/user/ttf/twocandidate"));
        FileOutputFormat.setOutputPath(job, new Path(output));



        return job;


    }
    public Job getOneCandiante(String input, String output, int maxNum)throws IOException{

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

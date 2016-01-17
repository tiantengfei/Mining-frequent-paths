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
    //path所要被分的块数
    private long pathBlocks;
    //每块中path的数目
   private long pathPerBlocks;

    //candidate所要被分成的块数
    private long candidateBlocks;
    //每块中candidate的数目
    private long candidatePerBlocks;



    public int run(String[] args) throws Exception {

        String input = args[0];
        String baseFile = "newPathCount/";
       // String output = args[1];


        //路径的频繁度
        int MaxNum = Integer.parseInt(args[1]);

        //查找的频繁路径长度
        int iteraNum = Integer.parseInt(args[2]);

        //每块中path的数目
        pathPerBlocks = Integer.parseInt(args[3]);

        //每块中candidate数目
        candidatePerBlocks = Integer.parseInt(args[4]);
        int currentItera = 0;



      // Job pathJob = getPathJob(input, "newPathCount/path");

      /**  pathJob.waitForCompletion(true);
       Counters pathJobCounters = pathJob.getCounters();
        //pathNum = pathJobCoun ters.findCounter("PATH_NUM", "num of path").getValue();
        Counter pathCounter =
                pathJobCounters.findCounter(MyCounter.PATH_NUM);
        pathNum = pathCounter.getValue();
       **/


        pathNum = 80000;
        pathBlocks = pathNum / pathPerBlocks + 1;

        //pathNum = getConf().getLong("PATH_NUM", 0);
        System.out.println("path num is " + pathNum);


        Job oneCandiantejob = getOneCandidate(baseFile + "test.txt", baseFile + "mini1", MaxNum);

        oneCandiantejob.waitForCompletion(true);

        Counters oneCandinateJobCounter =  oneCandiantejob.getCounters();
        caidinateNum = oneCandinateJobCounter.findCounter("CANDINATE_NUM", "candinateNum").getValue();
        System.out.println(" The candinateNum is " + caidinateNum);

        String candidate_in = baseFile + "mini1";
        String candidate_out = null;

        /**
        candidate_out = baseFile + "candidate" + 2;
        Job canidateJob = getTwoCandidate(candidate_in, candidate_out);
        canidateJob.waitForCompletion(true);

        caidinateNum = canidateJob.getCounters().findCounter("CANDINATE_NUM", "candinateNum")
                .getValue();

        System.out.println("candinateNum:"  + caidinateNum);
        candidateBlocks = caidinateNum / candidatePerBlocks + 1;
        String pathmini_out = baseFile + "mini" + 2;

       Job pathMiniJob = getPathMiniJob(baseFile + "test.txt",candidate_out, pathmini_out, MaxNum, 2);

        pathMiniJob.waitForCompletion(true);
        long num = pathMiniJob.getCounters().findCounter("path_num", "path of num").getValue();
        long matchNum =  pathMiniJob.getCounters().findCounter("matchNum", "match of num").getValue();
        System.out.println("pathnumis ....:" + num  + "\n" + "matchNUm...:" + matchNum);


        String getMiniout = baseFile + "finamini" + 2;
        String pathmini_out = baseFile + "mini" + 2;
        Job getMiniJob = getFinalMini(pathmini_out, getMiniout, MaxNum);
        getMiniJob.waitForCompletion(true);
        Job canidateJob = getCandidate(getMiniout, baseFile + "candidate3");
        canidateJob.waitForCompletion(true);
         **/

        while(currentItera <  iteraNum - 1 ) {

            int executeItera = currentItera + 2;
            Job canidateJob = null;
            Job getMiniJob = null;
            Job pathMiniJob = null;
            candidate_out = baseFile + "candidate" + executeItera;
            if (currentItera == 0) {
                System.out.println("twoCandidate............");

                canidateJob = getTwoCandidate(candidate_in, candidate_out);

            } else {
                System.out.println("noTwocandidate.................");
                canidateJob = getCandidate(candidate_in, candidate_out);
            }
            canidateJob.waitForCompletion(true);

            caidinateNum = canidateJob.getCounters().findCounter("CANDINATE_NUM", "candinateNum")
                    .getValue();

            candidateBlocks = caidinateNum / candidatePerBlocks + 1;
            String pathmini_out = baseFile + "mini" + executeItera;
            pathMiniJob = getPathMiniJob(baseFile + "test.txt", candidate_out, pathmini_out, MaxNum, executeItera);

            pathMiniJob.waitForCompletion(true);

            String getMiniout = baseFile + "finamini" + executeItera;

            getMiniJob = getFinalMini(pathmini_out, getMiniout, MaxNum);
            getMiniJob.waitForCompletion(true);
            candidate_in = getMiniout;

            ++currentItera;


        }


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
        getConf().setLong("PATH_PER_BLOCKS", pathPerBlocks);
        getConf().setLong("CANDINATE_PER_BLOCKS", candidatePerBlocks);

        getConf().setLong("PATH_BLOCKS", pathBlocks);
        getConf().setLong("CANDINATE_BLOCKS", candidateBlocks);
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

    public Job getFinalMini(String input, String output, int maxNum)throws IOException{

        getConf().setInt("MAX_NUM", maxNum);
        Job job = new Job(getConf(), "final Mini");

        job.setJarByClass(getClass());
        job.setMapperClass(GetMiniMapReduce.GetMiniMapper.class);
        job.setReducerClass(GetMiniMapReduce.GetMiniReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
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

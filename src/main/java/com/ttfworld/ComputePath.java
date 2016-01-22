package com.ttfworld;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

        //日志文件所在的位置
        String input = args[0];

        //得出的结果应该存储的根文件的位置
        String baseFile = "newPathCount/";


        //路径的频繁度
        int MaxNum = Integer.parseInt(args[1]);

        //查找的频繁路径长度
        int iteraNum = Integer.parseInt(args[2]);

        //每块中path的数目
        pathPerBlocks = Integer.parseInt(args[3]);

        //每块中candidate数目
        candidatePerBlocks = Integer.parseInt(args[4]);
        int currentItera = 0;

       //pathJob用于从日志文件中得到Path，为了测试方便。不再每次计算Path

        Job pathJob = getPathJob(input, "newPathCount/newpath");
       pathJob.waitForCompletion(true);
       Counters pathJobCounters = pathJob.getCounters();
        pathNum = pathJobCounters.findCounter("PATH_NUM", "num of path").getValue();
        Counter pathCounter =
                pathJobCounters.findCounter(MyCounter.PATH_NUM);
        pathNum = pathCounter.getValue();


        /**

        Job duringPathJob = duringPathJob(baseFile + "newpath", baseFile + "t1");
        duringPathJob.waitForCompletion(true);
        pathNum = duringPathJob.getCounters().
                findCounter("nums_period_time", "nums_time").getValue();

        System.out.println("num _ time :" + pathNum);

**/
        pathBlocks = pathNum / pathPerBlocks + 1;

        //pathNum = getConf().getLong("PATH_NUM", 0);
        System.out.println("path num is " + pathNum);


        Job oneMinijob = getmini1( baseFile + "/newpath", baseFile + "mini1", MaxNum);

        oneMinijob.waitForCompletion(true);

        Counters oneMiniJobCounter =  oneMinijob.getCounters();
        long mini1Num = oneMiniJobCounter.findCounter("CANDINATE_NUM", "candinateNum").getValue();
        System.out.println(" The Mini1 is " + mini1Num);

        String candidate_in = baseFile + "mini1";
        String candidate_out = null;


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
            pathMiniJob = getPathMiniJob(baseFile + "/newpath", candidate_out, pathmini_out, MaxNum, executeItera);

            pathMiniJob.waitForCompletion(true);

            String getMiniout = baseFile + "finamini" + executeItera;


            getMiniJob = getFinalMini(pathmini_out, getMiniout, MaxNum);
            getMiniJob.waitForCompletion(true);
            candidate_in = getMiniout;


            ++currentItera;


        }

        return 0;
    }

    /**
     * 得到2候选
     * @param input  输入路径
     * @param output 输出路径
     * @return
     * @throws IOException
     */
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

    /**
     * 得到候选
     * @param input 前一次finaminiJob的输出
     * @param output 候选文件
     * @return
     * @throws IOException
     */
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

    /**
     *找出每一块数据中路径的频发度
     * @param input  生成的Path文件
     * @param input2 候选文件
     * @param output
     * @param maxNum    频繁度
     * @param iteraNum  所要查找的频繁路径的长度
     * @return
     * @throws IOException
     */
    public Job getPathMiniJob(String input,String input2 , String output, int maxNum, int iteraNum) throws IOException{

        getConf().setInt("MAX_NUM", maxNum);
        getConf().setInt("ITEAR_NUM", iteraNum);
        getConf().setLong("PATH_PER_BLOCKS", pathPerBlocks);
        getConf().setLong("CANDINATE_PER_BLOCKS", candidatePerBlocks);

        getConf().setLong("PATH_BLOCKS", pathBlocks);
        getConf().setLong("CANDINATE_BLOCKS", candidateBlocks);
        Job job = new Job(getConf(), "PathMiniJob");
       // job.setNumReduceTasks(2);

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


    /**
     *得到最终的频繁路径
     * @param input  PathminiJob的输出文件
     * @param output
     * @param maxNum
     * @return
     * @throws IOException
     */
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

    /**
     * 得到长度为一的频繁路径
     * @param input  path路径
     * @param output
     * @param maxNum
     * @return
     * @throws IOException
     */
    public Job getmini1(String input, String output, int maxNum)throws IOException{

        getConf().setInt("MAX_NUM", maxNum);
        Job job = new Job(getConf(), "One Candiante");

        job.setJarByClass(getClass());
        job.setMapperClass(OneMiniMapReduce.OneCandinateMapper.class);
        job.setReducerClass(OneMiniMapReduce.OneCandinateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // job.setOutputKeyClass(Text.class);
        // job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));


        return job;

    }

    /**
     * 从日志得到路径
     * @param input  日志文件
     * @param output  path
     * @return
     * @throws IOException
     */
    public Job getPathJob(String input, String output) throws IOException{

        getConf().set("start_time", "2015-11-02 14:00:00");
        getConf().set("end_time", "2015-11-02 16:00:00");
        Job job = new Job(getConf(), "Path Job");
       // getConf().setLong("PATH_NUM", 0);

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

    public Job duringPathJob(String input, String output) throws IOException {


        //getConf().setLong("PATH_NUM", 0);

        getConf().setInt("test", 50);
        getConf().set("start_time", "2015-11-02 16:00:00");
        getConf().set("end_time", "2015-11-02 21:00:00");
        Job job = new Job(getConf(), "During Path Job");



        job.setJarByClass(getClass());
        job.setMapperClass(DuringPathMapReduce.DuringPathMapper.class);
        job.setReducerClass(DuringPathMapReduce.DuringPathReducer.class);

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

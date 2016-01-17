package com.ttfworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PathMiningMapper extends Mapper<Object,Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        //获取candinate的块数CANDINATE_BLOCKS
        long candinateBlocks = conf.getLong("CANDINATE_BLOCKS", 100L);

        //获取path的块数
        long pathBlocks = conf.getLong("PATH_BLOCKS", 100);

        //获取每块的path以及candidate数目   PATH_PER_BLOCKS
        long candinatePerBlocks = conf.getLong("CANDINATE_PER_BLOCKS", 50);
        long pathPerBlocks = conf.getLong("PATH_PER_BLOCKS", 50);

        //以candinate块的序号以及path块的序号作为key
        //若是path则需要传递到所有的candiandate块
        //若是candinate块则需要传递所有的path块
        String str = value.toString();
        if(str.endsWith("_c")){  //此路经为候选

            long canidateNum = getNum(str, false);
            long blockNum = canidateNum / candinatePerBlocks;

            for(long i = 0; i < pathBlocks; i ++)
                context.write(new Text("" + blockNum + "_" + i),
                        new Text(str));

        }else {

            long pathNum = getNum(str, true);
            long blockNum = pathNum / pathPerBlocks;

            context.getCounter("path_num", "path of num").increment(1);
            for(long i = 0; i < candinateBlocks; i ++)
                context.write(new Text("" + i + "_" + blockNum),
                        new Text(str));
        }




    }

    long getNum(String str, boolean isPath){

        String s = "";
        if(isPath)
            s = str.split("\t")[1];
        else {
            String numstr= str.split("\t")[1];
            s = numstr.substring(0, numstr.length() - 2);
        }

        return Long.parseLong(s);
    }


}

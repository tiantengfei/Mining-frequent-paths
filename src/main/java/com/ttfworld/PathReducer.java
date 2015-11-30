package com.ttfworld;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by ttf on 15-11-29.
 */
public class PathReducer extends Reducer<Text, Text, Text, NullWritable>{

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 存储会话开始时间
        String startTime = null;
        String endTime = null;

        ArrayList<String> tmpEdgeList = new ArrayList<String>();
        // 将一个key的所用路段 都存储起来，接下来按时间排序
        for (Text text : values) {
            tmpEdgeList.add(text.toString());
        }


        Object[] sortedEdgeList = tmpEdgeList.toArray();
        Arrays.sort(sortedEdgeList);
//session 时间
        //startTime = ((String) sortedEdgeList[0]).split("~")[0];
       // endTime = ((String) sortedEdgeList[sortedEdgeList.length - 1])
       //         .split("~")[0];

        String path="";
       // path存放的是t1~a~b~t2~b~c~
        for (int i = 0; i < sortedEdgeList.length; i++) {

            String str = (String)sortedEdgeList[i];
            String[] s = str.split("~");

            path  += s[1] +"~";

        }
//
      path = path.substring(0, path.length() - 1);
        //key: sesison~ip~country~device~browser~startTime~endTime
        //value: time~url~url
        System.out.println(path.toString());
        context.write(new Text(path), NullWritable.get());

    }// reduce
}

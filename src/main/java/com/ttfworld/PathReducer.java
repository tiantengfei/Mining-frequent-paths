package com.ttfworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */
public class PathReducer extends Reducer<Text, Text, Text, LongWritable>{

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 将一个key的所用路段 都存储起来，接下来按时间排序
        ArrayList<String> tmpEdgeList = new ArrayList<String>();
        for (Text text : values) {
            tmpEdgeList.add(text.toString());
        }
        Object[] sortedEdge = tmpEdgeList.toArray();
        Arrays.sort(sortedEdge);

        // 获得session的发生时间
        String time = ((String) sortedEdge[0]).split("~")[0];


        Configuration conf =  context.getConfiguration();

        String startTime = conf.get("start_time");
        String endTime = conf.get("end_time");

        if(time.compareTo(startTime) > 0 &&
                time.compareTo(endTime) < 0) {

            // 路径拆分
            List<ArrayList<String>> pathList = new ArrayList<ArrayList<String>>();

            for (Object str : sortedEdge) {
                String start = "";
                String end = "";
                // if (((String) str).split("~").length == 3) {
                start = ((String) str).split("~")[1];
                end = ((String) str).split("~")[2];
                // } else {
                // continue; // 过滤错误数据
                // }


                if (pathList.size() == 0) { // 首条拆分路径
                    List<String> path = new ArrayList<String>();
                    path.add(start);
                    path.add(end);
                    pathList.add((ArrayList<String>) path);

                    // continue;
                } else {
                    // 判断str,然后拼接
                    int k;
                    // all path per session
                    for (k = pathList.size() - 1; k >= 0; k--) {
                        List<String> path = pathList.get(k);
                        if (path.get(path.size() - 1).equals(start)) {
                            // a,b,c c,d >> a,b,c,d
                            path.add(end);
                            break; // end已经拼接到最近的path上
                        } else {
                            // 在最近的path上向前查找
                            // a,b,c b,g >> a,b,g && a,b,c
                            int index = -1;
                            for (int j = path.size() - 2; j >= 0; j--) {
                                if (path.get(j).equals(start)) {
                                    index = j;
                                    break;
                                }
                            }
                            if (index != -1) { // 找到并new path
                                List<String> newPath = new ArrayList<String>();
                                for (int j = 0; j <= index; j++) { // copy
                                    newPath.add(path.get(j));
                                }
                                newPath.add(end);
                                pathList.add((ArrayList<String>) newPath);
                                break; // 新path已添加
                            }
                        }
                    }// for pathList

                    // 尝试完已知的所有路径
                    // a,b,c m,f >> m,f && a,b,c 重新开一条路径
                    if (k == -1) {
                        List<String> newPath = new ArrayList<String>();
                        newPath.add(start);
                        newPath.add(end);
                        // streams 添加新path
                        pathList.add((ArrayList<String>) newPath);
                    }

                }

            }// for


            for (List<String> p : pathList) {

                String str ="";

                for (String s : p) {
                    str += s + "~";

                }

                context.getCounter(MyCounter.PATH_NUM).increment(1);

                Counter pathCounter =
                        context.getCounter(MyCounter.PATH_NUM);
                // long pathNum = context.getConfiguration().getLong("PATH_NUM", 0);
                long pathNum = pathCounter.getValue();
                context.write(new Text(str.substring(0, str.length() - 1)),
                        new LongWritable(pathNum));

            }
        }
    }// reduce
}

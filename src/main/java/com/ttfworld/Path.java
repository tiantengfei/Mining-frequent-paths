package com.ttfworld;

import java.util.*;

/**
 * Created by ttf on 15-11-29.
 *
 * 路径类：包括一条路径的主要信息
 */
public class Path {

    //flag=1,为候选； flag = 0, 为真实的路径
    private int flag;
    private List<String> pathList = new ArrayList<>();

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public Path(String pathStr){

        String[] str = pathStr.split(" ");

        if(str.length > 1)  flag = 1;
        else
            flag = 0;

        for(String path : str[0].split("~"))

            pathList.add(path);

    }



    public List<String> getKeys(int nums){

       // List<String> keys = new ArrayList<>();

        Set<String> keys = new HashSet<>();

        for(int i = 0; i < pathList.size() - nums + 1; i++){

            List<String> subpath = pathList.subList(i, i+ nums);
            String key = getKey(subpath);

            keys.add(key);
        }

        return new ArrayList<>(keys);
    }

    public String getKey(List<String> subpath){

        String k = "";
        for(String key : subpath)

            if(k.compareTo(key) < 0)
                k = key;


        return k;
    }

    public List<String> getPathList() {
        return pathList;
    }

    public void setPathList(List<String> pathList) {
        this.pathList = pathList;
    }

    public void add(String path){

        pathList.add(path);
    }
    public String getHead(){

        return pathList.get(0);
    }

    public String getTail(){

        return pathList.get(pathList.size() - 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String s : pathList) {
            sb.append(s + "~");
        }

        return sb.substring(0, sb.length() - 1).toString() + " " + flag;
    }
}

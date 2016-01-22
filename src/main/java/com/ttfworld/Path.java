package com.ttfworld;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ttf on 16-1-18.
 */

/**
 * Path类，用于辅助处理路径问题
 */
public class Path {

    private List<String> websites = new ArrayList<String>();

    private Boolean isCandidate;

    public Path(String pathStr){
        if(pathStr.endsWith("_c"))
            isCandidate = true;
        else
            isCandidate = false;

        String []str = pathStr.split("\t")[0].split("~");
        for(String s : str ){
            websites.add(s);
        }

    }

    public Path(List<String> websites, boolean isCandidate){
        this.isCandidate = isCandidate;
        this.websites = websites;
    }

    public Boolean getIsCandidate() {
        return isCandidate;
    }

    public void setIsCandidate(Boolean isCandidate) {
        this.isCandidate = isCandidate;
    }

    public List<String> getWebsites() {

        return websites;
    }

    public String getPath(){

        String str = "";
        for(String s : websites)

            str += s+"~";

        return str.substring(0, str.length() - 1);
    }
    public void setWebsites(List<String> websites) {
        this.websites = websites;
    }
}

package com.ttfworld;

/**
 * Created by ttf on 16-1-15.
 */

/**
 * 用于对路径的处理
 */
public class CandidateHelper {

    private static CandidateHelper candidateHelper;
    private CandidateHelper(){

    }

    public static CandidateHelper getInsance(){

        if(candidateHelper == null)
            candidateHelper = new CandidateHelper();

        return candidateHelper;
    }

    public  String getHead(String candidate){

        String s1 = candidate.substring(0, candidate.lastIndexOf("~"));
        String s2 = s1.substring(0, s1.lastIndexOf("~"));
        return s2;

    }

    public   String getTail(String candidate){

        String s1 = candidate.substring(candidate.indexOf("*")+ 1);

        return s1;
    }

    public String subtractHeadFromCandidate(String candidate){
        String s1 = candidate.substring(candidate.lastIndexOf("*") + 1);

        return s1;
    }

    public  String getNewCandinate(String c1 , String c2){

        return c1 +"~(.~)*" +subtractHeadFromCandidate(c2);

    }
}

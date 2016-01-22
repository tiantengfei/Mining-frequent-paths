package com.ttfworld;

/**
 * Created by ttf on 16-1-15.
 */

import org.apache.hadoop.io.Text;

import java.util.List;

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
        return s1;

    }

    public  String getTail(String candidate){

        String s1 = candidate.substring(candidate.indexOf("~")+ 1);
        return s1;
    }

    public String subtractHeadFromCandidate(String candidate){
        String s1 = candidate.substring(candidate.lastIndexOf("*") + 1);

        return s1;
    }

    /**
     * 由两个候选获得新的方法
     * @param c1
     * @param c2
     * @return
     */
    public  String getNewCandinate(String c1 , String c2){

        return c1 + c2.substring(c2.lastIndexOf("~"));

    }

    /**
     * 获取路径中该候选出现的次数
     * @param path
     * @param pIndex
     * @param candidate
     * @param cIndex
     * @return
     */
    int findFrequency(Path path, int pIndex ,Path candidate, int cIndex){

        if((candidate.getWebsites().size() - 1) == cIndex){

            int sum = 0;
            List<String> websites = path.getWebsites();
            for(int i = pIndex; i < websites.size(); i++){

                if(websites.get(i).equals(candidate.getWebsites().get(cIndex)))
                    ++sum;
            }

            return  sum;
        }


        int sum = 0;

        List<String> websites = path.getWebsites();
        for(int i = pIndex; i < websites.size(); i++){

            if(websites.get(i).equals(candidate.getWebsites().get(cIndex)))
                sum += findFrequency(path, i + 1, candidate, cIndex + 1 );

        }



        return sum;

    }

    public  boolean isContain(Path path, int pIndex ,Path candidate, int cIndex){


        List<String> p = path.getWebsites();
        List<String> can = candidate.getWebsites();


        for(int i = pIndex ; i < p.size(); i++)
            if(p.get(i).
                    equals(candidate.getWebsites().get(cIndex))) {


                if(cIndex == can.size() - 1) return true;

                return isContain(path, i + 1, candidate, cIndex + 1);
            }

        return false;
    }
    /**
     * 判断是否为候选
     * @param text
     * @return
     */
    public boolean iscandidate(Text text) {

        String s = text.toString();

        if (s.endsWith("_c"))
            return true;


        return false;

    }

}

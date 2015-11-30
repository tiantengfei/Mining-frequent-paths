package com.ttfworld;

/**
 * Created by ttf on 15-11-29.
 */
public enum IterationNumEnum {

    ITERATION_NUM_ENUM(1),
    MAX_PATH_NUM(0);

    private int iterationNum ;
    IterationNumEnum(int num){

        this.iterationNum = num;

    }

    public void setIterationNumEnum(int num) {
        this.iterationNum = num;
    }

    public int getIterationNum(){

        return iterationNum;
    }
}

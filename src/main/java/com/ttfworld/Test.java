package com.ttfworld;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ttf on 15-11-29.
 */
public class Test {

  public enum Day{
      ONE("ttf", 12);

      private String name;
      private int age;
      Day(String name, int value){
        this.name = name;
          this.age = value;

      }

      public int getAge(){
         return age;
      }

      public void set(int age){

          this.age = age;
      }
  }

    public static void main(String[] args) {
        for(Day d : Day.values())
            System.out.println(d);

        System.out.println(Day.ONE.getAge());

        Day.ONE.set(13);
        System.out.println(Day.ONE.getAge());




        List<String> list = new ArrayList<>();
        list.add("bbb");
        list.add("aaa");
        list.add("ccc");

       // String[] s = new String[]{"bbbb", "aaaa", "cccc"};

        Object[] obj = list.toArray();

        Arrays.sort(obj);

        for(Object ob  : obj )
            System.out.println(ob);

        String ss = "ss\ts";
        String []sa = ss.split(" ");
        System.out.println(sa[0]);


        Path path = new Path("cbmcylinders.en.made-in-china.com/product-list-20.html" + " 1");
        System.out.println("flag:" + path.getFlag());


//        int []a = new int[]{1,2,3,4,5,6,7,8,9,10};
//
//        for(int i : a){
//
//            System.out.println("i:" + i);
//            for(int j : a)
//                System.out.print(j);
//        }
//
        for(String s : list){

            System.out.println("----flag: " + s);
            for(String a : list)
                System.out.print(a + " ");
        }


        Path p = new Path("www.made-in-china.com/~www.made-in-china.com/ 1");
        System.out.println(p.toString());
        System.out.println("flag:" + p.getFlag());
        System.out.println("head:" + p.getTail());
        System.out.println(p.getHead());
        System.out.println(p.getKeys(2));
        System.out.println(p.getPathList().size());


    }
}

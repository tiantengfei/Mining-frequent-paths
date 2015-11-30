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


        Path p = new Path("www.made-in-china.com/products-search/hot-china-products/Interphone.html~www.made-in-china.com/products-search/find-china-products/0b0nolimit/Interphone-2.html~www.made-in-china.com/productdirectory.do?action=hunt&code=1455000000&code4BrowerHistory=EEnxEJQbMJmm&order=0&style=b&page=1&memberLevel=&asFlag=&comProvince=nolimit&propertyValues=&from=hunt&word=Interphone&mode=and&comName=&comCode=&subCode=&size=30&viewType=1&toTradeMarkets=&hotflag=0&viewMoreOrLessClass=viewMore&includePartSearch=true&isProdCom=&newFlag=false&sgsBaseFlag=&catOhter=&sgsMembershipFlag=&sizeHasChanged=0&minNumOrder=&comCity=nolimit~www.made-in-china." +
                "com/products-search/find-Interphone-Pager/0b0nolimit/Interphone-2.html");
        System.out.println(p.toString());
        System.out.println("flag:" + p.getFlag());
        System.out.println("head:" + p.getTail());
        System.out.println(p.getHead());
        System.out.println(p.getKeys(2));
        System.out.println(p.getPathList().size());


    }
}

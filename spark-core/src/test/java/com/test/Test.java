package com.test;

public class Test {

    public static void main(String[] args) {
        try{
            String[] s =  new String[]{"1"};
            // String[] s = null; // new String[]{"1"};

            System.out.println(s[2]);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(">>>>>>>>>>>>>>>");

    }
}

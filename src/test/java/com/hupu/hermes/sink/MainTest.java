package com.hupu.hermes.sink;

import java.text.SimpleDateFormat;

public class MainTest {

    public static void main(String[] args) {

        //total time minute = 0.18518333333333334    ,  10 秒钟
        long start = System.currentTimeMillis();
        for (long i = 0; i < 14000000L; i++) {     // 8 秒钟
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        }
        double minute = (System.currentTimeMillis() - start) / 60000.0;
        System.out.println("total time minute = " + minute);
    }
}

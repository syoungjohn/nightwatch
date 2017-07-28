package com.jzsec.rtc.util;

import com.jzsec.rtc.config.Configuration;
import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.List;

/**
 * Created by caodaoxi on 16-11-14.
 */
public class Test {


    public static void main(String[] args) {
        String path = "/risk/schema";
        System.out.println(path.substring(path.lastIndexOf("/") + 1));

//        System.out.println();
    }
}

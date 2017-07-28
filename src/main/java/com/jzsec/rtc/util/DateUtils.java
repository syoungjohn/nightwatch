package com.jzsec.rtc.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by caodaoxi on 16-4-17.
 */
public class DateUtils {
    private static SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getDateString(long timestamp) {
        return dayFormat.format(new Date(timestamp));
    }


    public static String getDateTime(long timestamp) {
        return sDateFormat.format(new Date(timestamp));
    }

    public static String getNowTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        return df.format(new Date());
    }

    public static String getCurrentTime() {
        return sDateFormat.format(new   java.util.Date());
    }

    public static Long getDateNumber(long timestamp){
        String dateString = dayFormat.format(new Date(timestamp));
        String dateNum = dateString.replaceAll("-", "");
        return Long.valueOf(dateNum);
    }


}

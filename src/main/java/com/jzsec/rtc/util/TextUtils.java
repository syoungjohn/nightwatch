package com.jzsec.rtc.util;

import com.jzsec.rtc.bean.Parameter;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by caodaoxi on 16-4-15.
 */
public class TextUtils {

    public static String generateMsg(Parameter parameter, String msgTemplate) {
        return null;
    }

    public static String replaceStrVar(Map<String, Object> map, String template){
        for (Object s : map.keySet()) {
            template = template.replaceAll("\\$\\{".concat(s.toString()).concat("\\}")
                    , map.get(s.toString()).toString());
        }
        return template;
    }
    public static boolean isNumeric(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if( !isNum.matches() ){
            return false;
        }
        return true;
    }
}

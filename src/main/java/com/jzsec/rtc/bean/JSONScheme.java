package com.jzsec.rtc.bean;

import net.sf.json.JSONObject;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Created by caodaoxi on 16-6-21.
 */
public class JSONScheme implements Scheme {

    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
    public List<String> outputFields = null;
    public JSONScheme(List<String> outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String jsonStr = null;
        if (ser.hasArray()) {
            int base = ser.arrayOffset();
            jsonStr = new String(ser.array(), base + ser.position(), ser.remaining());
        } else {
            jsonStr = new String(Utils.toByteArray(ser), UTF8_CHARSET);
        }
        JSONObject jsonObject = JSONObject.fromObject(jsonStr);
        Values values = new Values();
        for (String outputField : outputFields) {
            if("jsonBody".equals(outputField)) {
                values.add(jsonStr);
            } else {
                if(!jsonObject.containsKey(outputField)) {
                    JSONObject rcMsgpara = JSONObject.fromObject(jsonObject.get("rc_msg_para"));
                    values.add(rcMsgpara.get(outputField));
                } else {
                    values.add(jsonObject.get(outputField));
                }
            }
        }
        return values;
    }

    @Override
    public Fields getOutputFields() {
        if(outputFields.size() == 0) return null;
        return new Fields(outputFields);
    }
}

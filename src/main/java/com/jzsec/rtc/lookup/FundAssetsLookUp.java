package com.jzsec.rtc.lookup;

import com.jzsec.rtc.util.RedisLookUp;
import net.sf.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by caodaoxi on 16-7-13.
 */
public class FundAssetsLookUp {

    public static Map<String, Object> getFundAssetsByYmtCode(Object ymtCode) {
        Object record = RedisLookUp.get("kg_asset:" + ymtCode);
        Map<String, Object> assets = new HashMap<String, Object>();
        assets.put("ymt_code", ymtCode);
        if(record == null) {
            assets.put("assets", 0);
            return assets;
        }
        assets.put("assets", Double.parseDouble(record.toString().trim()));
        return assets;
    }

    public static Map<String, Object> getFundAssetsByYmtCodeMetadata(){
        Map<String, Object> assetsMetadata = new HashMap<String, Object>();
        assetsMetadata.put("ymt_code", String.class);
        assetsMetadata.put("assets", Double.class);
        return assetsMetadata;
    }


    public static Map<String, Object> getStockDataByStockCode(String stockCode, String market) {
        Object record = RedisLookUp.get("kg_stock:" + stockCode + "_" + market);
//        System.out.println("kg_stock:" + stockCode + "_" + market);
        Map<String, Object> stockDataMap = new HashMap<String, Object>();
        stockDataMap.put("stockCode", stockCode);
        if(record == null) {
            stockDataMap.put("market", 0); //所属市场
//            stockDataMap.put("stkcode", "0"); //证券代码
            stockDataMap.put("stkname", "-"); //证券名称
                stockDataMap.put("cirqty", 0L);  //流通股本
            stockDataMap.put("preCloseprice", 0); //前天收盘价
            stockDataMap.put("maxRiseValue", 0);  //今日涨停价
            stockDataMap.put("maxDownValue", 0);  //今日跌停价

                stockDataMap.put("stFlag", 0);   //ST标识
            stockDataMap.put("stFlag2", 0);  //*ST标识
            stockDataMap.put("ptFlag", 0);   //PT标识
            stockDataMap.put("fxFlag", 0);   //高风险警示标识
            stockDataMap.put("delistingFlag", 0); //退市整理期标识

            stockDataMap.put("", 0); //复牌标识
            stockDataMap.put("quitFlag", 0);  //已退市标识

            stockDataMap.put("delistingStartTime", "0"); //退市整理期开始日期
            stockDataMap.put("delistingEndTime", "0");  //退市整理期结束日期
        } else {
            JSONObject recordJSON = JSONObject.fromObject(record);
//            String[] recordFields = record.toString().split(",");
            stockDataMap.put("market", recordJSON.getInt("market")); //所属市场
//            stockDataMap.put("stkcode", recordFields[1]); //证券代码
            stockDataMap.put("stkname", recordJSON.getString("stkname")); //证券名称
            stockDataMap.put("cirqty", recordJSON.getLong("cirqty"));  //流通股本
            stockDataMap.put("preCloseprice", recordJSON.getDouble("closeprice")); //前天收盘价
            stockDataMap.put("maxRiseValue", recordJSON.getDouble("maxRiseValue"));  //今日涨停价
            stockDataMap.put("maxDownValue", recordJSON.getDouble("maxDownValue"));  //今日跌停价

            stockDataMap.put("stFlag", recordJSON.getInt("stFlag"));   //ST标识
            stockDataMap.put("stFlag2", recordJSON.getInt("stFlag2"));  //*ST标识
            stockDataMap.put("ptFlag", recordJSON.getInt("ptFlag"));   //PT标识
            stockDataMap.put("fxFlag", recordJSON.getInt("fxFlag"));   //高风险警示标识
            stockDataMap.put("delistingFlag", recordJSON.getInt("delistingFlag")); //退市整理期标识

            stockDataMap.put("resumeFlag", recordJSON.getInt("resumeFlag")); //复牌标识
            stockDataMap.put("quitFlag", recordJSON.getInt("quitFlag"));  //已退市标识

            stockDataMap.put("delistingStartTime", recordJSON.getString("delistingStartTime")); //退市整理期开始日期
            stockDataMap.put("delistingEndTime", recordJSON.getString("delistingEndTime"));  //退市整理期结束日期
        }
        System.out.println(JSONObject.fromObject(stockDataMap).toString());
        return stockDataMap;
    }

    public static Map<String, Object> getStockDataByStockCodeMetadata(){
        Map<String, Object> stockDataMetadata = new HashMap<String, Object>();
        stockDataMetadata.put("market", Integer.class); //所属市场
        stockDataMetadata.put("stockCode", String.class); //所属市场
        stockDataMetadata.put("stkname", String.class); //证券名称
        stockDataMetadata.put("cirqty", Long.class);  //流通股本
        stockDataMetadata.put("preCloseprice", Double.class); //前天收盘价
        stockDataMetadata.put("maxRiseValue", Double.class);  //今日涨停价
        stockDataMetadata.put("maxDownValue", Double.class);  //今日跌停价

        stockDataMetadata.put("stFlag", Integer.class);   //ST标识
        stockDataMetadata.put("stFlag2", Integer.class);  //*ST标识
        stockDataMetadata.put("ptFlag", Integer.class);   //PT标识
        stockDataMetadata.put("fxFlag", Integer.class);   //高风险警示标识
        stockDataMetadata.put("delistingFlag", Integer.class); //退市整理期标识

        stockDataMetadata.put("resumeFlag", Integer.class); //复牌标识
        stockDataMetadata.put("quitFlag", Integer.class);  //已退市标识

        stockDataMetadata.put("delistingStartTime", String.class); //退市整理期开始日期
        stockDataMetadata.put("delistingEndTime", String.class);  //退市整理期结束日期
        return stockDataMetadata;
    }

    public static void main(String[] args) {
        Map<String, Object> assets = FundAssetsLookUp.getStockDataByStockCode("600030", "1");
        System.out.println(assets.size());
//        System.out.println(assets.get("assets"));
    }
}

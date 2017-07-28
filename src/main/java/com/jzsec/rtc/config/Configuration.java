package com.jzsec.rtc.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Created by caodaoxi on 16-4-17.
 */
public class Configuration {
    private static org.apache.commons.configuration.Configuration config = null;
    private static String defultConfigFilePath = "rtc.properties";
    public static void init(String configFilePath) {
        try {
            defultConfigFilePath = configFilePath;
            config = new PropertiesConfiguration(defultConfigFilePath);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public static org.apache.commons.configuration.Configuration getConfig() {
        if(config == null) {
            init(defultConfigFilePath);
        }
        return config;
    }

}

package com.wls.learn.service.bean;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author:wangpeng
 * @Date: 2022/12/1
 * @Description: 装载配置文件response.properties的对象
 * @version:1.0
 */

@Component
@ConfigurationProperties(prefix ="response" )
@PropertySource("classpath:response.properties")
public class ResponseCodePropertyConfig {

    public Map<String,String> codes =new ConcurrentHashMap<>();
    public Map<String, String> getCodes() {
        return codes;
    }
    public void setCodes(Map<String, String> codes) {
        this.codes = codes;
    }
    public String getMsg(String code){
        return this.codes.get(code);
    }
}

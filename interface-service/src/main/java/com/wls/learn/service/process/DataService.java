package com.wls.learn.service.process;


import javax.servlet.http.HttpServletRequest;

/**
 * @Author:wangpeng
 * @Date: 2022/12/1
 * @Description: 数据采集服务器中的业务接口
 * @version:1.0
 */
public interface DataService {

    void process(String dataType, HttpServletRequest request) throws Exception;
}

package com.wls.learn.service.process.impl;

import com.wls.learn.service.bean.CustomException;
import com.wls.learn.service.bean.ResponseCodePropertyConfig;
import com.wls.learn.service.constant.ResponseConstant;
import com.wls.learn.service.process.DataService;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedInputStream;
import java.nio.charset.StandardCharsets;

/**
 * @Author:wangpeng
 * @Date: 2022/12/1
 * @Description: ***
 * @version:1.0
 */
@Service("dataService")
public class DataServiceImpl implements DataService {

    @Autowired
    ResponseCodePropertyConfig config;

    Logger logger = LoggerFactory.getLogger(DataServiceImpl.class);


    /**
     * 接受数据，并通过Logger保存数据到文件中
     * @param dataType 数据类型
     * @param request 请求对象
     * @throws Exception
     */
    @Override
    public void process(String dataType, HttpServletRequest request) throws Exception {
        //保护性的判断
        if (StringUtils.isEmpty(dataType)) {//数据类型没有传入，抛出异常
            throw new CustomException(ResponseConstant.CODE_0001, config.getMsg(ResponseConstant.CODE_0001),dataType);
        }
        int contentLength = request.getContentLength();
        //判断请求头中是否传入数据
        if (contentLength < 1 ) {
            throw new CustomException(ResponseConstant.CODE_0002, config.getMsg(ResponseConstant.CODE_0002),dataType);
        }

        //从Request中读取数据
        byte[] datas = new byte[contentLength]; //存放数据的字节数组

        BufferedInputStream input = new BufferedInputStream(request.getInputStream());

        //最大尝试读取的次数
        int tryTime=0;
        //最大尝试读取次数内，最终读取的数据长度
        int totalRealLeagth=0;

        int maxTryTime=100; //容错性
        while( totalRealLeagth < contentLength && tryTime < maxTryTime) {
            int readLength = input.read(datas, totalRealLeagth, contentLength-totalRealLeagth);

            if (readLength < 0) {
                throw new CustomException(ResponseConstant.CODE_0007, config.getMsg(ResponseConstant.CODE_0007), dataType);
            }
            totalRealLeagth += readLength;

            if(totalRealLeagth == contentLength) {
                break;
            }

            tryTime++;
            Thread.sleep(200);
        }
        if (totalRealLeagth < contentLength) { //经过多处的读取，数据任然没有读完
            throw new CustomException(ResponseConstant.CODE_0007, config.getMsg(ResponseConstant.CODE_0007), dataType);
        }

        input.close();
        String jsonStr = new String(datas, StandardCharsets.UTF_8);
        logger.info(jsonStr);
    }
}

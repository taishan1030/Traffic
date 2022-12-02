package com.wls.learn.service.controller;

import com.wls.learn.service.bean.ResponseCodePropertyConfig;
import com.wls.learn.service.bean.ResponseEntity;
import com.wls.learn.service.constant.ResponseConstant;
import com.wls.learn.service.process.DataService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author:wangpeng
 * @Date: 2022/12/1
 * 数据采集服务器的控制器
 * http://localhost:8686/controller/sendData/carInfo
 * 数据的具体内容通过请求头传入
 */
@RestController
@RequestMapping("/controller")
public class DataController {

    @Autowired
    DataService dataService;

    @Autowired
    private ResponseCodePropertyConfig config;

    @PostMapping("/sendData/{dataType}")
    public Object collect(@PathVariable("dataType") String dataType, HttpServletRequest request) {
        dataService.process(dataType, request);
        return new ResponseEntity(ResponseConstant.CODE_0000, config.getMsg(ResponseConstant.CODE_0000), dataType);
    }
}

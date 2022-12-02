package com.wls.learn.service.bean;

/**
 * @Author:wangpeng
 * @Date: 2022/12/2
 * @Description: 字定义异常
 * @version:1.0
 */
public class CustomException extends Exception {
    private String code;
    private String message;
    private String dataType;

    public CustomException(String msg){
        super(msg);
    }

    public CustomException(String code, String message, String dataType) {
        this.code = code;
        this.message = message;
        this.dataType = dataType;
    }

    public CustomException(String message, String code, String message1, String dataType) {
        super(message);
        this.code = code;
        this.message = message1;
        this.dataType = dataType;
    }

    public CustomException(String message, Throwable cause, String code, String message1, String dataType) {
        super(message, cause);
        this.code = code;
        this.message = message1;
        this.dataType = dataType;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }
}

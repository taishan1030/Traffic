package com.wls.learn.service;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.Charset;

/**
 * @Author:wangpeng
 * @Date: 2022/12/2
 * @Description: ***
 * @version:1.0
 */
public class SendDataToServer {

    public static void main(String[] args) {
        String url = "http://localhost:8686/controller/sendData/triffic_data";
        HttpClient client = HttpClients.createDefault();
        String result = null;
        try{
            int i = 0;
            while(i<20) {
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type","application/json");
                String data = "11,22,33,44," + i;
                post.setEntity(new StringEntity(data, Charset.forName("UTF-8")));
                HttpResponse response = client.execute(post);
                i++;
                Thread.sleep(1000);
                if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    result = EntityUtils.toString(response.getEntity(),"UTF-8");
                }
                System.out.println(result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

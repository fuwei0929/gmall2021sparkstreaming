package com.atguigu.gmall.gmall_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.constants.MyConstants;
import lombok.extern.log4j.Log4j;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Smexy on 2021/9/1
 *      收客户端发送的日志，收成功后，不需要返回客户端页面的！
 *
 *
 *      客户端发送的url:
 *              http://localhost:8089/gmall_logger/applog
 *
 *              配置端口: 8089
 *              项目名:  gmall_logger
 *
 *      携带的参数名:
 *                默认为param
 *
 */
@RestController
@Log4j // Logger log = Logger.getLogger(LogController.class);
public class LogController {

    @Autowired
    private KafkaTemplate producer;

    @RequestMapping(value="/applog")
    public Object handleLog(String param){

        //System.out.println(param);
        log.info(param);

        //将字符串转换为JSONObject对象
        JSONObject jsonObject = JSON.parseObject(param);

        if (jsonObject.getString("start") != null){

            producer.send(MyConstants.STARTUP_LOG, param );

        }

        if (jsonObject.getString("actions") != null){

            producer.send(MyConstants.ACTIONS_LOG, param );

        }

        if (jsonObject.getString("displays") != null){

            producer.send(MyConstants.DISPLAY_LOG, param );

        }

        if (jsonObject.getString("err") != null){

            producer.send(MyConstants.ERROR_LOG, param );
        }

        if (jsonObject.getString("page") != null){
            producer.send(MyConstants.PAGE_LOG, param );
        }

        return "sucess";
    }


    @RequestMapping(value="/hello")
    public Object handle1(){

        System.out.println("hello");

        return "hello";

    }



}

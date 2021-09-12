package com.atguigu.gmall.gmall_publisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmall_publisher.beans.DAUPerHour;
import com.atguigu.gmall.gmall_publisher.beans.GMVPerHour;
import com.atguigu.gmall.gmall_publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Smexy on 2021/9/3
 */
@RestController //只返回数据，不会返回页面
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    /*

    异构数据库的查询：
          mybatis查询   hbase 和 mysql

    http://localhost:8070/  realtime-total  ?date=2021-08-15

    [{"id":"dau","name":"当日日活数","value":1200},
{"id":"new_mid","name":"新增设备数","value":233}]

        统计完GMV：

    [{"id":"dau","name":"当日日活数","value":1200},
{"id":"new_mid","name":"新增设备数","value":233},
{"id":"order_amount","name":新增交易额，"value":gmv交易额}

]


            [] :  List , JSONArray
     */
    @RequestMapping(value = "/realtime-total")
    public Object handle1(String date){

        Integer dauToday = publisherService.getDAUByDate(date);
        Integer newMidCount = publisherService.getNewMidCountByDate(date);
        Double gmvByDate = publisherService.getGMVByDate(date);

        ArrayList<JSONObject> result = new ArrayList<>();

        JSONObject jsonObject1 = new JSONObject();
        JSONObject jsonObject2 = new JSONObject();
        JSONObject jsonObject3 = new JSONObject();

        jsonObject1.put("id","dau");
        jsonObject1.put("name","当日日活数");
        jsonObject1.put("value",dauToday);

        jsonObject2.put("id","new_mid");
        jsonObject2.put("name","新增设备数");
        jsonObject2.put("value",newMidCount);

        jsonObject3.put("id","order_amount");
        jsonObject3.put("name","新增交易额");
        jsonObject3.put("value",gmvByDate);

        result.add(jsonObject1);
        result.add(jsonObject2);
        result.add(jsonObject3);

        return result;

    }


    /*
    http://localhost:8070/   realtime-hours   ?  id=dau  &  date=2021-08-15

    {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}

            {} :  Map , JSONObject


            "11":383: 抽象为数据模型DAUPerHour


          http://localhost:8070/   realtime-hours   ?  id=order_amount  &  date=2021-08-15

     */
    @RequestMapping(value = "/realtime-hours")
    public Object handle2(String date,String id){

        // 根据当日日期求昨天日期
        String yesTodayStr = LocalDate.parse(date).minusDays(1).toString();

        JSONObject result = new JSONObject();

        if (id.equals("dau")){
            List<DAUPerHour> todayData = publisherService.getDAUPerHourOfDate(date);
            List<DAUPerHour> yesTodayData = publisherService.getDAUPerHourOfDate(yesTodayStr);



            result.put("yesterday",handleDAUData(yesTodayData));
            result.put("today",handleDAUData(todayData));

        }else  if (id.equals("order_amount")){

            List<GMVPerHour> todayGMVData = publisherService.getGMVPerHourOfDate(date);
            List<GMVPerHour> yesTodayGMVData = publisherService.getGMVPerHourOfDate(yesTodayStr);

            result.put("today",handleGMVData(todayGMVData));
            result.put("yesterday",handleGMVData(yesTodayGMVData));


        }

        return result;

    }

    @RequestMapping(value = "/sale_detail")
    public JSONObject getESData(String date,Integer startpage,Integer size,String keyword) throws IOException {

        return  publisherService.getESData(date,startpage,size,keyword);

    }

    public JSONObject  handleDAUData(List<DAUPerHour> todayData){
        JSONObject result = new JSONObject();

        for (DAUPerHour data : todayData) {

            result.put(data.getHour(),data.getDau());

        }

        return result;

    }

    public JSONObject  handleGMVData(List<GMVPerHour> todayData){
        JSONObject result = new JSONObject();

        for (GMVPerHour data : todayData) {

            result.put(data.getHour(),data.getGmv());

        }

        return result;

    }

}

package com.atguigu.gmall.gmall_publisher.service;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmall_publisher.beans.DAUPerHour;
import com.atguigu.gmall.gmall_publisher.beans.GMVPerHour;

import java.io.IOException;
import java.util.List;

/**
 * Created by Smexy on 2021/9/3
 */
public interface PublisherService {

    //查询当日日活
    Integer getDAUByDate(String date);

    //查询当日新增设备数
    Integer getNewMidCountByDate(String date);

    // 查询某一天，各时段的DAU
    List<DAUPerHour> getDAUPerHourOfDate(String date);

    //查询当日GMV
    Double getGMVByDate(String date);

    // 查询某一天，各时段的GMV
    List<GMVPerHour> getGMVPerHourOfDate(String date);

    //查询明细
    JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException;
}

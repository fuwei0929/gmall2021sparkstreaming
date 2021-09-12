package com.atguigu.gmall.gmall_publisher.service;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmall_publisher.beans.DAUPerHour;
import com.atguigu.gmall.gmall_publisher.beans.GMVPerHour;
import com.atguigu.gmall.gmall_publisher.dao.ESDao;
import com.atguigu.gmall.gmall_publisher.mappers.DAUMapper;
import com.atguigu.gmall.gmall_publisher.mappers.GMVMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * Created by Smexy on 2021/9/3
 */
@Service
public class PublisherServiceImpl implements  PublisherService {

    @Autowired
    private DAUMapper dauMapper;

    @Autowired
    private GMVMapper gmvMapper;

    @Autowired
    private ESDao esDao;

    @Override
    public Integer getDAUByDate(String date) {

        System.out.println("业务1....");
        System.out.println("业务2....");
        System.out.println("业务3....");
        System.out.println("业务4....");

        return dauMapper.getDAUByDate(date);
    }

    @Override
    public Integer getNewMidCountByDate(String date) {

        System.out.println("业务1....");
        System.out.println("业务2....");
        System.out.println("业务3....");
        System.out.println("业务4....");

        return dauMapper.getNewMidCountByDate(date);

    }

    @Override
    public List<DAUPerHour> getDAUPerHourOfDate(String date) {
        System.out.println("业务1....");
        System.out.println("业务2....");
        System.out.println("业务3....");
        System.out.println("业务4....");

        return dauMapper.getDAUPerHourOfDate(date);
    }

    @Override
    public Double getGMVByDate(String date) {
        System.out.println("业务1....");
        System.out.println("业务2....");
        System.out.println("业务3....");
        System.out.println("业务4....");
        return gmvMapper.getGMVByDate(date);
    }

    @Override
    public List<GMVPerHour> getGMVPerHourOfDate(String date) {
        System.out.println("业务1....");
        System.out.println("业务2....");
        System.out.println("业务3....");
        System.out.println("业务4....");
        return gmvMapper.getGMVPerHourOfDate(date);
    }

    @Override
    public JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException {
        System.out.println("业务1....");
        System.out.println("业务2....");
        System.out.println("业务3....");
        System.out.println("业务4....");
        return esDao.getESData(date,startpage,size,keyword);
    }
}

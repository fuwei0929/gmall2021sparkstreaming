package com.atguigu.gmall.gmall_publisher.mappers;

import com.atguigu.gmall.gmall_publisher.beans.DAUPerHour;
import com.baomidou.dynamic.datasource.annotation.DS;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by Smexy on 2021/9/3
 */
@DS("hbase")
@Repository
public interface DAUMapper {

    //查询当日日活
    Integer getDAUByDate(String date);

    //查询当日新增设备数
    Integer getNewMidCountByDate(String date);

    // 查询某一天，各时段的DAU
    List<DAUPerHour> getDAUPerHourOfDate(String date);

}

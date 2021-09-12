package com.atguigu.gmall.gmall_publisher.mappers;

import com.atguigu.gmall.gmall_publisher.beans.GMVPerHour;
import com.baomidou.dynamic.datasource.annotation.DS;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by Smexy on 2021/9/6
 */
@DS("mysql")
@Repository
public interface GMVMapper {

    //查询当日日活
    Double getGMVByDate(String date);

    // 查询某一天，各时段的DAU
    List<GMVPerHour> getGMVPerHourOfDate(String date);

}

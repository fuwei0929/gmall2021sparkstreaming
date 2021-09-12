package com.atguigu.gmall.gmall_publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Smexy on 2021/9/3
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GMVPerHour {

    private  String hour;
    private  Double gmv;
}

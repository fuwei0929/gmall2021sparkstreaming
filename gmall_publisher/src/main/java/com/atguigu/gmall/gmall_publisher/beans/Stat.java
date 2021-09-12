package com.atguigu.gmall.gmall_publisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Created by Smexy on 2021/9/8
 */
@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Option> options;
}

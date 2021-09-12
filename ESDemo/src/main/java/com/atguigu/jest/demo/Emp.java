package com.atguigu.jest.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by Smexy on 2021/9/7
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {

    private String empid;
    private Integer age;
    private Double balance;
    private String name;
    private String gender;
    private String hobby;

}

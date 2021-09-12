package com.atguigu.constants;

/**
 * Created by Smexy on 2021/9/1
 *
 *      接口不允许被实例化
 */
public interface MyConstants {

    // 用户行为数据(和离线数仓的数据一模一样)的五个分类
    String STARTUP_LOG = "STARTUP_LOG";
    String ERROR_LOG = "ERROR_LOG";
    String DISPLAY_LOG = "DISPLAY_LOG";
    String PAGE_LOG = "PAGE_LOG";
    String ACTIONS_LOG = "ACTIONS_LOG";


    // 和业务数据需求相关的常量
    String GMALL_ORDER_INFO = "GMALL_ORDER_INFO";
    String GMALL_ORDER_DETAIL = "GMALL_ORDER_DETAIL";
    String GMALL_USER_INFO = "GMALL_USER_INFO";
}

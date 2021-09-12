package com.atguigu.jest.demo;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * Created by Smexy on 2021/9/7
 *
 *      套路：
 *              ①创建客户端
 *              ②连接 ES 服务端
 *              ③准备要执行的操作
 *                      读：
 *                              Search
 *
 *                      写：
 *                              增|改 ： Index
 *                              删：     Delete
 *
 *                              批量操作： Bulk
 *
 *               ④发送命令
 *               ⑤接收返回
 *                      读： 接收返回值
 *                      写：  可以不接收
 *
 *        ------------------
 *        ES的Jest中大量使用两种设计模式：
 *              ①工厂模式：
 *                          获取A对象，可以通过  AFactory.newInstance()
 *
 *              ②建筑者模式：
 *                         获取A对象，可以通过 ABuiler.setXxx().setXxx().build()
 *
 */
public class WriteDemo1 {

    public static void main(String[] args) throws IOException {

        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig=(new HttpClientConfig.Builder("http://hadoop102:9200")).build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // ①创建客户端
        JestClient jestClient = jestClientFactory.getObject();

        String source="{\n" +
                "  \"empid\": 2001,\n" +
                "  \"age\": 20,\n" +
                "  \"balance\": 2000,\n" +
                "  \"name\": \"李小三\",\n" +
                "  \"gender\": \"男\",\n" +
                "  \"hobby\": \"吃饭睡觉\"\n" +
                "}";

        //构造 写操作对象  添加员工:
        Index index = new Index.Builder(source)
                .index("test")
                .type("emps")
                .id("2001")
                .build();

        DocumentResult result = jestClient.execute(index);


        //关闭
        jestClient.close();


    }
}

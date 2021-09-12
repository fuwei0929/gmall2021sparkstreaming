package com.atguigu.jest.demo;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;

import java.io.IOException;


public class WriteDemo3 {

    public static void main(String[] args) throws IOException {

        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig=(new HttpClientConfig.Builder("http://hadoop102:9200")).build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // ①创建客户端
        JestClient jestClient = jestClientFactory.getObject();

        // 新增一个2003号员工
        Emp emp = new Emp("2003", 31, 2222.22, "李大四", "男", "喝酒");

        Index index = new Index.Builder(emp)
                .index("test")
                .type("emps")
                .id("2003")
                .build();
        //删除 2002号员工
        Delete delete = new Delete.Builder("2002")
                .index("test")
                .type("emps")
                .build();


        //构造 批量执行的对象 Bulk
        Bulk bulk = new Bulk.Builder().addAction(index).addAction(delete).build();


        BulkResult execute = jestClient.execute(bulk);

        System.out.println(execute.getItems().size());


        //关闭
        jestClient.close();


    }
}

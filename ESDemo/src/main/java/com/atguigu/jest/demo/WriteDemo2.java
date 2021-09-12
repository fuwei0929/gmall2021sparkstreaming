package com.atguigu.jest.demo;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;

import java.io.IOException;


public class WriteDemo2 {

    public static void main(String[] args) throws IOException {

        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig=(new HttpClientConfig.Builder("http://hadoop102:9200")).build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // ①创建客户端
        JestClient jestClient = jestClientFactory.getObject();

        Emp emp = new Emp("2002", 30, 2222.22, "李小四", "男", "喝酒");

        //构造 写操作对象  添加员工:
        Index index = new Index.Builder(emp)
                .index("test")
                .type("emps")
                .id("2002")
                .build();

        DocumentResult result = jestClient.execute(index);


        //关闭
        jestClient.close();


    }
}

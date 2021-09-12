package com.atguigu.jest.demo;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;


public class ReadDemo1 {

    public static void main(String[] args) throws IOException {

        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig=(new HttpClientConfig.Builder("http://hadoop102:9200")).build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        // ①创建客户端
        JestClient jestClient = jestClientFactory.getObject();

        String source="{\n" +
                "   \"query\": {\n" +
                "     \"match_all\": {}\n" +
                "   },\n" +
                "   \"aggs\": {\n" +
                "     \"gendercount\": {\n" +
                "       \"terms\": {\n" +
                "         \"field\": \"gender.keyword\",\n" +
                "         \"size\": 2\n" +
                "       }\n" +
                "     }\n" +
                "   }\n" +
                "  \n" +
                "  \n" +
                "}";

        Search search = new Search.Builder(source)
                .addIndex("test")
                .addType("emps").build();

        //查询的结果
        SearchResult searchResult = jestClient.execute(search);

        System.out.println("hits.total:"+searchResult.getTotal());
        System.out.println("hits.max_score:"+searchResult.getMaxScore());
        List<SearchResult.Hit<Emp, Void>> hits = searchResult.getHits(Emp.class);

        for (SearchResult.Hit<Emp, Void> hit : hits) {

            System.out.println("_type"+hit.type);
            System.out.println("_index"+hit.index);
            System.out.println("_id"+hit.id);
            System.out.println("_socre"+hit.score);
            System.out.println("_source"+hit.source);
        }

        //-------------------遍历aggs------------------
        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation gendercount = aggregations.getTermsAggregation("gendercount");

        List<TermsAggregation.Entry> buckets = gendercount.getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println(bucket.getKey() + ":" + bucket.getCount());
        }


        //关闭
        jestClient.close();


    }
}

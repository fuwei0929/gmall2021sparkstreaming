package com.atguigu.gmall.gmall_publisher.dao;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.gmall_publisher.beans.Option;
import com.atguigu.gmall.gmall_publisher.beans.SaleDetail;
import com.atguigu.gmall.gmall_publisher.beans.Stat;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Smexy on 2021/9/8
 */
@Repository
public class ESDaoImpl implements  ESDao {

    @Autowired
    private JestClient jestClient;

    @Override
    public JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException {

        SearchResult searchResult = query(date, startpage, size, keyword);

        List<SaleDetail> detail = getDetail(searchResult);

        Stat ageStat = parseAgeStat(searchResult);
        Stat genderStat = parseGenderStat(searchResult);

        ArrayList<Stat> stats = new ArrayList<>();

        stats.add(ageStat);
        stats.add(genderStat);

        JSONObject jsonObject = new JSONObject();

        jsonObject.put("total",searchResult.getTotal());
        jsonObject.put("stats",stats);
        jsonObject.put("detail",detail);

        return jsonObject;
    }


    /*
            从ES查询数据

            date：  指定从哪天的index上查询

            startpage: 查询的是哪一页的数据
                             startpage = 1        startpage = 2

                             from : 0              10

                             from = (startpage - 1) * size

                             size : 10

                             size = 10

            size ： 一页有多少条

            keyword: 查询的关键字


            ---------------------
            GET /gmall2021_sale_detail2021-09-08/_search
{
 "query": {
   "match": {
     "sku_name": "小米手机"
   }
 },
 "aggs": {
   "gendercount": {
     "terms": {
       "field": "user_gender",
       "size": 2
     }
   },
    "agecount": {
     "terms": {
       "field": "user_age",
       "size": 150
     }
   }
 },
  "from": 0
  , "size": 10


}




     */
    public SearchResult query(String date, Integer startpage, Integer size, String keyword) throws IOException {

        //生成indexname
        String indexName= "gmall2021_sale_detail" + date;

        int from = (startpage - 1) * size;

        //match部分
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword);

        //aggs部分
        TermsBuilder termsBuilder1 = AggregationBuilders.terms("gendercount").field("user_gender").size(2);
        TermsBuilder termsBuilder2 = AggregationBuilders.terms("agecount").field("user_age").size(150);

        //构造query部分
        String querySource = new SearchSourceBuilder().query(matchQueryBuilder).aggregation(termsBuilder1).aggregation(termsBuilder2).from(from).size(size).toString();

        Search search = new Search.Builder(querySource).addType("_doc").addIndex(indexName).build();

        SearchResult searchResult = jestClient.execute(search);


        return searchResult;
    }


    public List<SaleDetail> getDetail( SearchResult searchResult){

        List<SaleDetail> result =new ArrayList<>();

        List<SearchResult.Hit<SaleDetail, Void>> hits = searchResult.getHits(SaleDetail.class);

        for (SearchResult.Hit<SaleDetail, Void> hit : hits) {

            SaleDetail saleDetail = hit.source;

            saleDetail.setEs_metadata_id(hit.id);

            result.add(saleDetail);

        }
        return result;

    }

    public Stat parseAgeStat(SearchResult searchResult) {

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation agecount = aggregations.getTermsAggregation("agecount");

        List<TermsAggregation.Entry> buckets = agecount.getBuckets();

        int lt20count = 0;
        int gt30count = 0;
        int from20to30count = 0;

        for (TermsAggregation.Entry bucket : buckets) {

            if (Integer.parseInt(bucket.getKey()) < 20){

                lt20count += bucket.getCount();

            }else if (Integer.parseInt(bucket.getKey()) >= 30){

                gt30count += bucket.getCount();
            }else{

                from20to30count += bucket.getCount();
            }

        }

        //计算概率

        double sumCount = lt20count + gt30count + from20to30count;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        String lt20Percent = decimalFormat.format(lt20count / sumCount * 100);
        String gt30Percent = decimalFormat.format(gt30count / sumCount * 100);

        String from20to30Percent = decimalFormat.format(100 - Double.parseDouble(lt20Percent) - Double.parseDouble(gt30Percent));

        List<Option> options=new ArrayList<>();

        options.add(new Option("20岁到30岁",from20to30Percent));
        options.add(new Option("30岁及30岁以上",gt30Percent));
        options.add(new Option("20岁以下",lt20Percent));

        Stat stat = new Stat("用户年龄占比", options);

        return stat;


    }

    public Stat parseGenderStat(SearchResult searchResult) {

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation agecount = aggregations.getTermsAggregation("gendercount");

        List<TermsAggregation.Entry> buckets = agecount.getBuckets();

        long maleCount = 0;
        long femaleCount = 0;


        for (TermsAggregation.Entry bucket : buckets) {

            if (bucket.getKey().equals("M")){

                maleCount = bucket.getCount();

            }else{

                femaleCount = bucket.getCount();
            }

        }

        //计算概率

        double sumCount = maleCount + femaleCount ;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        String malePercent = decimalFormat.format(maleCount / sumCount * 100);


        String femalePercent = decimalFormat.format(100 - Double.parseDouble(malePercent) );

        List<Option> options=new ArrayList<>();

        options.add(new Option("男",malePercent));
        options.add(new Option("女",femalePercent));


        Stat stat = new Stat("用户性别占比", options);

        return stat;


    }


}

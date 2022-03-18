package com.example.springesjd.service;

import com.alibaba.fastjson.JSON;
import com.example.springesjd.pojo.Good;
import com.example.springesjd.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;  //加上@@Qualifier,就可以用client去替换restHighLevelClient
    // client就相当于kibana

    public Boolean parseContent(String keywords) throws IOException {
        /*------------------1.新建索引jd_goods--------------*/
        GetIndexRequest request = new GetIndexRequest("jd_goods");
        boolean exists = client.indices().exists(request,RequestOptions.DEFAULT);
        if(!exists){  //如果不存在，就新建索引库
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("jd_goods");
            CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            System.out.println("创建索引："+response);
        }

        /*------------------ 2.解析网页--------------------*/
        List<Good> goodList = new HtmlParseUtil().parseJD(keywords);

        /*------3.将爬取的数据添加到es中（批量添加文档操作-------*/
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");

        for(int i =0; i<goodList.size();i++){
            bulkRequest.add(
                    new IndexRequest("jd_goods")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(goodList.get(i)), XContentType.JSON)
            );
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        return !bulkResponse.hasFailures();  //加了!,返回true表示成功
    }


    public ArrayList<Map<String,Object>> searchContent(String keywords, int pageOn, int pageSize) throws IOException {
        if(pageOn<=1){
            pageOn=1;
        }
        /*-----------------文档搜索--------------------*/
        //创建search请求
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        //创建构建器对象，封装对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchSourceBuilder.from(pageOn);  //分页
        searchSourceBuilder.size(pageSize);
        //精确匹配
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", keywords);
        searchSourceBuilder.query(termQueryBuilder);
        //client发起请求
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();  //为了返回new的对象
        for (SearchHit documentFields : searchResponse.getHits().getHits()) {
            list.add(documentFields.getSourceAsMap());
        }
        return list;

    }
}

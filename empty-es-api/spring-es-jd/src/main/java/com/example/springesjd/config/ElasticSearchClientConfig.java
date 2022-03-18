package com.example.springesjd.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration  //相当于这个client的xml配置文件-bean
public class ElasticSearchClientConfig {

    @Bean  // bean id= class=
    public RestHighLevelClient restHighLevelClient (){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9200, "http")));
        return client;
    }

}

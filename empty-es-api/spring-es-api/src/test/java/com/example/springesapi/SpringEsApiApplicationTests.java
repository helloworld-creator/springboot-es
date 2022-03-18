package com.example.springesapi;

import com.alibaba.fastjson.JSON;
import com.example.springesapi.pojo.User;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class SpringEsApiApplicationTests {
	@Autowired
	@Qualifier("restHighLevelClient")
	private RestHighLevelClient client;  //加上@@Qualifier,就可以用client去替换restHighLevelClient
										 // client就相当于kibana

	@Test
	void contextLoads() {

	}
	//测试 创建索引 Request
	@Test
	void testCreateIndex() throws IOException {
		//1.定义 创建索引请求
		CreateIndexRequest request = new CreateIndexRequest("user");
		//2.client发送请求，获得响应
		CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);

		System.out.println(response);
	}

	//测试 获取索引 Request
	@Test
	void testGetIndex() throws IOException {
		//1.定义 获取索引请求
		GetIndexRequest request = new GetIndexRequest("test2");
		//2.client发送请求，返回布尔值
		boolean exists = client.indices().exists(request,RequestOptions.DEFAULT);

		System.out.println(exists);
	}

	//测试 删除索引
	@Test
	void testDeleteIndex() throws IOException {
		//1.定义 删除索引请求
		DeleteIndexRequest request = new DeleteIndexRequest("User");
		//2.client发送请求，获得响应
		AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

		System.out.println(response.isAcknowledged()); //响应的状态为true就代表成功删除
	}

	//测试 添加文档
	@Test
	void testAddDocument() throws IOException {

		//1.创建请求连接索引对象
		IndexRequest request = new IndexRequest("user");
		//2.匹配规则： put /User/_doc/id, 封装request对象
		request.id("zxf");
		request.timeout(TimeValue.timeValueSeconds(1)); //最长等待时间，法一
		request.timeout("1s");  //法二

		//3.创建文档对象
		User user = new User("zxf", 36);
		//4.将请求转化为json格式
		request.source(JSON.toJSONString(user), XContentType.JSON);

		//5.向索引中添加数据
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		//打印结果
		System.out.println(indexResponse.toString());
		System.out.println(indexResponse.status());
	}

	//测试 文档是否存在
	@Test
	void testExistDocument() throws IOException {
		GetRequest getRequest = new GetRequest("user", "zxf");
		//getRequest.fetchSourceContext(new FetchSourceContext(false));  // 过滤掉_source上下文的信息
		//getRequest.storedFields("_none_");
		boolean exists = client.exists(getRequest,RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	//测试 获取文档信息
	@Test
	void testGetDocument() throws IOException {
		GetRequest getRequest = new GetRequest("user", "zxf");
		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		System.out.println(getResponse);  //返回的信息内容和kibana中一致
		System.out.println(getResponse.getSource()); //只返回source字段，以Map格式返回
		System.out.println(getResponse.getSourceAsString()); //只返回source字段，以字符串格式返回
	}

	//测试 更新文档
	@Test
	void testUpdateDocument() throws IOException {
		// 创建更新请求对象，封装对象
		UpdateRequest updateRequest = new UpdateRequest("user", "zxf");
		updateRequest.timeout("1s");

		// 创建文档对象，转为json格式封装更新请求对象
		User user = new User("zxf", 37);
		updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

		//client发起请求，返回响应对象
		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
		System.out.println(updateResponse.status());
	}

	//测试 删除文档
	@Test
	void testDeleteDocument() throws IOException {
		DeleteRequest deleteRequest = new DeleteRequest("user", "zxf");
		DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
		System.out.println(deleteResponse.status());
	}

	//测试 批量添加文档
	@Test
	void testBulkAddDocument() throws IOException {
		//1. 创建请求对象，封装对象
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s"); //根据需求（感觉）设置

		//2.创建文档对象列表
		ArrayList<User> userArrayList = new ArrayList<>();
		userArrayList.add(new User("zxf1", 11)) ;
		userArrayList.add(new User("zxf2", 12)) ;
		userArrayList.add(new User("zxf3", 13)) ;
		userArrayList.add(new User("zxf4", 14)) ;
		userArrayList.add(new User("zxf5", 15)) ;
		//批处理请求
		for (int i=0;i<userArrayList.size();i++){
			bulkRequest.add(
					new DeleteRequest("user")
			);
		}

		//3.client发起批量请求
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulkResponse.hasFailures()); //输出是否失败，false为成功
	}

	//测试 批量删除文档（一） 无循环
	@Test
	void testBulkDeleteDocument() throws IOException {
		//1. 创建批量请求对象,封装对象
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s");

		//2.批处理请求
		bulkRequest.add(
				new DeleteRequest("user","FOJ4i38BZC1sB52QAx4r")
		);
		bulkRequest.add(
				new DeleteRequest("user","FeJ4i38BZC1sB52QAx4r")
		);
		bulkRequest.add(
				new DeleteRequest("user","FuJ4i38BZC1sB52QAx4r")
		);
		bulkRequest.add(
				new DeleteRequest("user","F-J4i38BZC1sB52QAx4r")
		);
		bulkRequest.add(
				new DeleteRequest("user","GOJ4i38BZC1sB52QAx4r")
		);

		//client发起批处理请求
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulkResponse.hasFailures());
	}

	//测试 批量更新文档
	@Test
	void testBulkUpdateDocument() throws IOException {
		//1. 创建请求对象，封装对象
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s"); //根据需求（感觉）设置

		//2.创建文档对象列表
		ArrayList<User> userArrayList = new ArrayList<>();
		userArrayList.add(new User("ff1", 101)) ;
		userArrayList.add(new User("ff2", 102)) ;
		userArrayList.add(new User("ff3", 103)) ;
		userArrayList.add(new User("ff4", 104)) ;
		userArrayList.add(new User("ff5", 105)) ;
		//批处理请求
		for (int i=0;i<userArrayList.size();i++){
			bulkRequest.add(
					new UpdateRequest("user","")  //id空着就行，但是不能不写
					.id(""+(i+1))
					.doc(JSON.toJSONString(userArrayList.get(i)),XContentType.JSON)
			);
		}

		//3.client发起批量请求
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulkResponse.hasFailures()); //输出是否失败，false为成功
	}

	//测试 批量删除文档（二） 循环
	@Test
	void testBulkDeleteDocument2() throws IOException {
		//1. 创建批量请求对象,封装对象
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("10s");

		//2.批处理请求
		for(int i=0;i<5;i++){
			bulkRequest.add(
					new DeleteRequest("user","")  //id空着就行，但是不能不写
					.id(""+(i+1))
			);
		}

		//client发起批处理请求
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		System.out.println(bulkResponse.hasFailures());
	}

	//测试 搜索
	@Test
	void testSearch() throws IOException {
		//1.创建搜索请求对象
		SearchRequest searchRequest = new SearchRequest("test1");

		//2.构造搜索条件，封装搜索请求对象
		SearchSourceBuilder searchSourceBuildersourceBuilder = new SearchSourceBuilder();  //搜索条件构造器
			//QueryBuilders.termQuery 精确匹配
			//QueryBuilders.matchAllQuery() 匹配所有
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "morning");
		//MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
		searchSourceBuildersourceBuilder.query(termQueryBuilder);
		searchSourceBuildersourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		searchSourceBuildersourceBuilder.from(0); //分页
		searchSourceBuildersourceBuilder.size(3); //分页

		searchRequest.source(searchSourceBuildersourceBuilder);

		//3.client发起搜索请求
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(JSON.toJSONString(searchResponse.getHits()) );

	}
}

package top.moxingwang.elasticsearch;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * EsTest1
 */
public class ElasticsearchTest1 {
    Logger logger = LoggerFactory.getLogger(ElasticsearchTest1.class);

    RestHighLevelClient client;

    @Before
    public void init() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        //配置x-pack需要认证的情况
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("trade", "trade12345"));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("172.18.71.37", 9200))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        client = new RestHighLevelClient(restClientBuilder);
    }

    @Test
    public void query() throws IOException {
        SearchRequest searchRequest = new SearchRequest("trade-order-test");
        searchRequest.types("data");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(10);

        searchRequest.source(searchSourceBuilder);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "mo"));
        searchSourceBuilder.query(queryBuilder);


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        logger.info("命中数量{}最大分值{}", totalHits, maxScore);

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            logger.info("获取到数据ID{},详细数据{}", searchHit.getId(), JSON.toJSONString(searchHit.getSourceAsMap()));
        }
    }

    @Test
    public void upsert() throws IOException {
        //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.4/java-rest-high-document-update.html
        UpdateRequest updateRequest = new UpdateRequest("trade-order-test", "data", "1");

        String strJson = "{\n" +
                "  \"name\": \"mo\",\n" +
                "  \"cluster_name\": \"my-application\",\n" +
                "  \"cluster_uuid\": \"FBG-jnLfS4KD3ANeXOsTNg\",\n" +
                "  \"version\": {\n" +
                "    \"number\": \"7.2.0\",\n" +
                "    \"build_flavor\": \"default\",\n" +
                "    \"build_type\": \"tar\",\n" +
                "    \"build_hash\": \"508c38a\",\n" +
                "    \"build_date\": \"2019-06-20T15:54:18.811730Z\",\n" +
                "    \"build_snapshot\": false,\n" +
                "    \"lucene_version\": \"8.0.0\",\n" +
                "    \"minimum_wire_compatibility_version\": \"6.8.0\",\n" +
                "    \"minimum_index_compatibility_version\": \"6.0.0-beta1\"\n" +
                "  },\n" +
                "  \"tagline\": \"You Know, for Search\"\n" +
                "}";

        updateRequest.doc(strJson, XContentType.JSON);
        updateRequest.upsert(strJson, XContentType.JSON);

        RestStatus restStatus = client.update(updateRequest, RequestOptions.DEFAULT).status();
        System.out.println(restStatus);
    }
}

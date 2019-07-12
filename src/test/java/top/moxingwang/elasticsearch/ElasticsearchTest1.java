package top.moxingwang.elasticsearch;

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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * EsTest1
 */
public class ElasticsearchTest1 {

    @Test
    public void t1() throws IOException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("trade", "trade12345"));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("esuc.dev.rs.com", 19200))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

        // boolean response = client.ping(RequestOptions.DEFAULT);
        /*{
            ClusterHealthRequest request = new ClusterHealthRequest();
            ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        }*/

        /*{
            IndexRequest indexRequest = new IndexRequest("trade-order-test", "type");

            indexRequest.source("{\n" +
                    "  \"_doc\": {\n" +
                    "    \"properties\": {\n" +
                    "      \"message\": {\n" +
                    "        \"type\": \"text\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", XContentType.JSON);
            try {
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                System.out.println(indexResponse);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/

        {
            SearchRequest searchRequest = new SearchRequest("trade-order-sales_0");
            searchRequest.types("type");

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(searchRequest);

        }

        {
            SearchRequest searchRequest = new SearchRequest("trade-order-sales_0");
            searchRequest.types("type");

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.from(1);
            searchSourceBuilder.size(10);

            searchRequest.source(searchSourceBuilder);


            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            SearchHits hits = searchResponse.getHits();
            long totalHits = hits.getTotalHits();

            float maxScore = hits.getMaxScore();

            SearchHit[] searchHits = hits.getHits();

            System.out.println(1);


        }


        {
            //https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.4/java-rest-high-document-update.html

            String jsonString = "{\"created\":\"2017-01-01\"}";
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("updated", new Date());
            jsonMap.put("reason", "daily update11111111111111111");
            UpdateRequest updateRequest = new UpdateRequest("trade-order-test", "data", "2");

            updateRequest.doc(jsonMap);
            updateRequest.upsert(jsonMap);
//            updateRequest.upsert(jsonString, XContentType.JSON);

            RestStatus restStatus = client.update(updateRequest,RequestOptions.DEFAULT).status();
            System.out.println(restStatus);

           /* UpdateResponse updateResponse = client.prepareUpdate("trade-order-test", "type", 1)
//                            .setVersionType(VersionType.FORCE)
//                            .setVersion(System.currentTimeMillis())
                    .setDoc("{}")
                    .setDocAsUpsert(true)
                    .execute()
                    .actionGet();

            if (updateResponse.isCreated()) {
                logger.info("ES重入{}次执行创建{}", retry, afterTable.getId());
            } else {
                logger.info("ES重入{}次执行更新{}", retry, afterTable.getId());
            }*/
        }

        System.out.println(1);

    }
}

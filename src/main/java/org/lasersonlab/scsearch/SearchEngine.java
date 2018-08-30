package org.lasersonlab.scsearch;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;

/**
 * A facade to a search engine (Elasticsearch). Exposes methods to index and search documents, and to delete a whole
 * index.
 */
public class SearchEngine {

    public static void index(Client client, String index, String type, String jsonObject) {
        IndexResponse response = client.prepareIndex(index, type)
                .setSource(jsonObject, XContentType.JSON).get();
        if (response.getResult() != DocWriteResponse.Result.CREATED) {
            throw new IllegalStateException("Document was not created");
        }
    }

    public static void bulkIndex(Client client, String index, String type, List<String> jsonObjects) {
        BulkRequest request = new BulkRequest();
        for (String jsonObject : jsonObjects) {
            request.add(new IndexRequest(index, type).source(jsonObject, XContentType.JSON));
        }
        BulkResponse response = client.bulk(request).actionGet();
        if (response.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    System.out.println(failure.getMessage());
                }
            }
            throw new IllegalStateException("Failures while bulk indexing");
        }
    }

    public static void refreshIndex(Client client, String... indices) {
        RefreshRequest request = new RefreshRequest(indices);
        client.admin().indices().refresh(request).actionGet();
    }

    public static SearchHits search(Client client, QueryBuilder query, String... indices) {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchRequest.indices(indices);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        return searchResponse.getHits();
    }

    public static void deleteIndex(Client client, String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        client.admin().indices().delete(request);
    }

}

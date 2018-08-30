package org.lasersonlab.scsearch;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.List;

public class SearchEngine {

    public static void index(Client client, String index, String type, String jsonObject) {
        IndexResponse response = client.prepareIndex(index, type).setTimeout("1s")
                .setSource(jsonObject, XContentType.JSON).get();
        if (response.getResult() != DocWriteResponse.Result.CREATED) {
            throw new IllegalStateException("Document was not created");
        }
    }

    public static void refreshIndex(Client client, String... indices) {
        RefreshRequest request = new RefreshRequest(indices);
        client.admin().indices().refresh(request).actionGet();
    }

    public static List<SearchHit> search(Client client, QueryBuilder query) {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest).actionGet();
        return Arrays.asList(searchResponse.getHits().getHits());
    }

    public static void deleteIndex(Client client, String index) {
        DeleteIndexRequest request = new DeleteIndexRequest(index);
        client.admin().indices().delete(request);
    }

}

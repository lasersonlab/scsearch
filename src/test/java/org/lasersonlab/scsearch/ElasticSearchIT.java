package org.lasersonlab.scsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ElasticSearchIT {

    private static final String INDEX = "cells";
    private static final String TYPE = "cell";

    private static Client client;

    @BeforeClass
    public static void setUp() throws Exception {
        Settings settings = Settings.builder()
                .put("client.transport.sniff", true).build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        SearchEngine.deleteIndex(client, INDEX);
        Thread.sleep(1000); // wait for deletion
    }

    @Test
    @Ignore("requires a local elastic search instance")
    public void testAll() {
        String[] cellDocs = {
                "{\"id\":0,\"genes\":[0, 2]}",
                "{\"id\":1,\"genes\":[1, 3, 4]}",
                "{\"id\":2,\"genes\":[1, 2, 3]}",
        };

        SearchEngine.bulkIndex(client, INDEX, TYPE, Arrays.asList(cellDocs));
        SearchEngine.refreshIndex(client, INDEX);

        QueryBuilder queryAll = QueryBuilders.matchAllQuery();
        SearchHit[] searchHits1 = SearchEngine.search(client, queryAll, INDEX).getHits();
        assertEquals(cellDocs.length, searchHits1.length);

        QueryBuilder q1 = QueryBuilders.matchQuery("genes", 1);
        QueryBuilder q2 = QueryBuilders.matchQuery("genes", 2);
        QueryBuilder query12 = QueryBuilders.boolQuery().must(q1).must(q2);
        SearchHit[] searchHits2 = SearchEngine.search(client, query12, INDEX).getHits();
        assertEquals(1, searchHits2.length);
        assertEquals(cellDocs[2], searchHits2[0].getSourceAsString());
    }
}

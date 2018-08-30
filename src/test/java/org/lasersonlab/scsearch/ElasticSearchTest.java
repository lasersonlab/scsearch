package org.lasersonlab.scsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.BeforeClass;
import org.junit.Test;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ElasticSearchTest {

    private static final String INDEX = "cells";
    private static final String TYPE = "cell";

    private static Client client;

    @BeforeClass
    public static void setUp() throws Exception {
        Settings settings = Settings.builder()
                .put("client.transport.sniff", true).build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        SearchEngine.deleteIndex(client, "10x");
        SearchEngine.deleteIndex(client, INDEX);
        Thread.sleep(1000); // wait for deletion
    }

    @Test
    public void testAll() {
        String[] cellDocs = {
                "{\"id\":0,\"genes\":[0, 2]}",
                "{\"id\":1,\"genes\":[1, 3, 4]}",
                "{\"id\":2,\"genes\":[1, 2, 3]}",
        };

        SearchEngine.bulkIndex(client, INDEX, TYPE, Arrays.asList(cellDocs));
        SearchEngine.refreshIndex(client, INDEX);

        QueryBuilder queryAll = QueryBuilders.matchAllQuery();
        List<SearchHit> searchHits1 = SearchEngine.search(client, queryAll);
        assertEquals(cellDocs.length, searchHits1.size());

        QueryBuilder q1 = QueryBuilders.matchQuery("genes", 1);
        QueryBuilder q2 = QueryBuilders.matchQuery("genes", 2);
        QueryBuilder query12 = QueryBuilders.boolQuery().must(q1).must(q2);
        List<SearchHit> searchHits2 = SearchEngine.search(client, query12);
        assertEquals(1, searchHits2.size());
        assertEquals(cellDocs[2], searchHits2.get(0).getSourceAsString());
    }

    @Test
    public void test10x() throws IOException, InvalidRangeException {
        String file = "/Users/tom/workspace/hdf5-java-cloud/files/1M_neurons_filtered_gene_bc_matrices_h5.h5";
        int totalShards = 320;
        int numShards = 3;
        for (int i = 0; i < numShards; i++) {
            // index each shard with a single bulk call
            List<TenX.Cell> cells = TenX.readShard(file, totalShards, i);
            List<String> cellDocs = cells.stream().map(TenX.Cell::toJson).collect(Collectors.toList());
            SearchEngine.bulkIndex(client, "10x", "10x", cellDocs);
            System.out.println(i);
        }
    }
}

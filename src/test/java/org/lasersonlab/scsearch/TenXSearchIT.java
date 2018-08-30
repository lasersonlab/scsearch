package org.lasersonlab.scsearch;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.BeforeClass;
import org.junit.Test;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TenXSearchIT {
    private static final String FILE = "files/1M_neurons_filtered_gene_bc_matrices_h5.h5";
    private static final String INDEX = "10x";
    private static final String TYPE = "10x";

    private static Client client;

    private static boolean index = false;

    @BeforeClass
    public static void setUp() throws Exception {
        Settings settings = Settings.builder()
                .put("client.transport.sniff", true).build();
        client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
        if (index) {
            SearchEngine.deleteIndex(client, INDEX);
            Thread.sleep(1000); // wait for deletion
        }
    }

    @Test
    public void testIndex10x() throws IOException, InvalidRangeException {
        assumeTrue(index);
        int totalShards = 320;
        int numShards = 3; // or 256 to do ~1M cells, any larger and we get an exception due to int overflow
        long start = System.currentTimeMillis();
        for (int i = 0; i < numShards; i++) {
            // index each shard with a single bulk call
            List<TenX.Cell> cells = TenX.readShard(FILE, totalShards, i);
            List<String> cellDocs = cells.stream().map(TenX.Cell::toJson).collect(Collectors.toList());
            SearchEngine.bulkIndex(client, INDEX, TYPE, cellDocs);
            System.out.println(i);
        }
        long end = System.currentTimeMillis();
        long totalTimeSec = (end - start) / 1000;
        System.out.println("Total time (s) " + totalTimeSec);
        System.out.println("Estimated time (s) " + (totalShards * ((double) totalTimeSec) / numShards));
    }

    @Test
    public void testCount10X() {
        QueryBuilder queryAll = QueryBuilders.matchAllQuery();
        long hitsCount = SearchEngine.search(client, queryAll, INDEX).getTotalHits();
        assertTrue(hitsCount > 0);
    }

    @Test
    public void testSearch10X() throws IOException {
        List<String> allGenes = TenX.getGeneNames(FILE);
        String commonGene = "ENSMUSG00000050708";
        String rareGene1 = "ENSMUSG00000095742";
        String rareGene2 = "ENSMUSG00000073242";
        long commonGeneNumHits = numHits(ImmutableList.of(commonGene), allGenes);
        long rareGene1NumHits = numHits(ImmutableList.of(rareGene1), allGenes);
        long rareGene2NumHits = numHits(ImmutableList.of(rareGene2), allGenes);
        long rareGene1And2NumHits = numHits(ImmutableList.of(rareGene1, rareGene2), allGenes);

        assertTrue(commonGeneNumHits > rareGene1NumHits);
        assertTrue(commonGeneNumHits > rareGene2NumHits);
        assertTrue(rareGene1NumHits > rareGene1And2NumHits);
        assertTrue(rareGene2NumHits > rareGene1And2NumHits);
    }

    private long numHits(List<String> genes, List<String> allGenes) {
        QueryBuilder queryAll = genesQuery(genes, allGenes);
        return SearchEngine.search(client, queryAll, INDEX).getTotalHits();
    }

    private QueryBuilder genesQuery(List<String> genes, List<String> allGenes) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        for (String gene : genes) {
            boolQuery.must(QueryBuilders.matchQuery("genes", allGenes.indexOf(gene)));
        }
        return boolQuery;
    }

}

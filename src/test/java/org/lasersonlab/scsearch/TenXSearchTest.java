package org.lasersonlab.scsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class TenXSearchTest {
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
        String file = "/Users/tom/workspace/hdf5-java-cloud/files/1M_neurons_filtered_gene_bc_matrices_h5.h5";
        int totalShards = 320;
        int numShards = 3;
        for (int i = 0; i < numShards; i++) {
            // index each shard with a single bulk call
            List<TenX.Cell> cells = TenX.readShard(file, totalShards, i);
            List<String> cellDocs = cells.stream().map(TenX.Cell::toJson).collect(Collectors.toList());
            System.out.println("Indexing " + cellDocs.size());
            SearchEngine.bulkIndex(client, "10x", "10x", cellDocs);
            System.out.println(i);
        }
    }

    @Test
    public void testCount10X() {
        QueryBuilder queryAll = QueryBuilders.matchAllQuery();
        long hitsCount = SearchEngine.search(client, queryAll, INDEX).getTotalHits();
        assertEquals(12283, hitsCount);
    }

}

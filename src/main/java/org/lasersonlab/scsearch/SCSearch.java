package org.lasersonlab.scsearch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SCSearch {

  @Parameter(names = {"--host", "-h"}, description = "The Elasticsearch hostname.")
  String host = "127.0.0.1";

  @Parameter(names = {"--port", "-p"}, description = "The Elasticsearch port number.")
  int port = 9300;

  @Parameter(names = {"--index"}, description = "The Elasticsearch index name")
  String index = "10x";

  @Parameter(names = {"--operation", "-o"}, description = "The operation to perform: 'index', 'search', or 'delete'")
  String operation = "search";

  @Parameter(names = {"--file"}, description = "The HDF5 file holding the SC data in 10x format. Used during indexing and also searching (for gene names).")
  String file;

  @Parameter(names = {"--total-shards"}, description = "The total number of shards to break the input into when indexing.")
  int totalShards;

  @Parameter(names = {"--num-shards"}, description = "The number of shards of the total to index. Allows you to index a subset of the total.")
  int numShards;

  @Parameter(description = "The gene names to search")
  List<String> genes = new ArrayList<>();

  public static void main(String... argv) throws Exception {
    SCSearch search = new SCSearch();
    JCommander.newBuilder()
        .addObject(search)
        .build()
        .parse(argv);
    search.run();
  }

  public void run() throws Exception {
    switch (operation) {
      case "index":
        if (numShards == 0) {
          numShards = totalShards;
        }
        System.out.printf("Creating index %s with %s out of %s shards\n", index, numShards, totalShards);
        index();
        break;
      case "search":
        System.out.printf("Search index %s for genes %s\n", index, genes);
        search();
        break;
      case "delete":
        System.out.printf("Deleting index %s\n", index);
        delete();
        break;
      default:
        System.out.println("Unrecognized operation: " + operation);
    }
  }

  private Client getClient() throws UnknownHostException {
    Settings settings = Settings.builder()
        .put("client.transport.sniff", true).build();
    return new PreBuiltTransportClient(settings)
        .addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
  }

  private void index() throws IOException, InvalidRangeException {
    try (Client client = getClient()) {
      for (int i = 0; i < numShards; i++) {
        // index each shard with a single bulk call
        List<TenX.Cell> cells = TenX.readShard(file, totalShards, i);
        List<String> cellDocs = cells.stream().map(TenX.Cell::toJson).collect(Collectors.toList());
        SearchEngine.bulkIndex(client, index, index, cellDocs);
        System.out.print('.');
      }
      System.out.println();
    }
  }

  private void search() throws IOException {
    try (Client client = getClient()) {
      List<String> allGenes = TenX.getGeneNames(file);
      QueryBuilder queryAll = genesQuery(genes, allGenes);
      SearchHits searchHits = SearchEngine.search(client, queryAll, index);
      System.out.printf("Matching cells: %d\n", searchHits.totalHits);
      int maxToShow = 10;
      for (int i = 0; i < Math.min(searchHits.totalHits, maxToShow); i++) {
        SearchHit searchHit = searchHits.getHits()[i];
        Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
        System.out.printf("\tbarcode=%s\n", sourceAsMap.get("barcode"));
      }
      if (searchHits.totalHits > maxToShow) {
        System.out.println("\t...");
      }
    }
  }

  private QueryBuilder genesQuery(List<String> genes, List<String> allGenes) {
    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
    for (String gene : genes) {
      boolQuery.must(QueryBuilders.matchQuery("genes", allGenes.indexOf(gene)));
    }
    return boolQuery;
  }

  private void delete() throws IOException {
    try (Client client = getClient()) {
      SearchEngine.deleteIndex(client, index);
    }
  }
}

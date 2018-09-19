# scsearch

Index and search single cell data.

## Installation

```bash
mvn install
alias scsearch='java -jar target/scsearch-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
```

[Install Elasticsearch] locally.

You'll need some [10x data](https://support.10xgenomics.com/single-cell-gene-expression/datasets/1.3.0/1M_neurons) too.

## Run

First create an index. This takes 15 minutes or so (for around 1 million cells).
(Note that we only create 256 out of 320 shards to avoid an as-yet-unresolved int overflow error in netcdf.)

```bash
scsearch -o index \
    --index 10x \
    --file files/1M_neurons_filtered_gene_bc_matrices_h5.h5 \
    --total-shards 320 \
    --num-shards 256
```

Now we can do a search to find cells that have non-zero expression levels for all genes in
a query set.
(Note that we need to supply the 10x file since it used as a source of
 all the gene names.)

```bash
scsearch --file files/1M_neurons_filtered_gene_bc_matrices_h5.h5 ENSMUSG00000050708 
Search index 10x for genes [ENSMUSG00000050708]
Matching cells: 1046148
	barcode=GGAATAACACCTCGTT-3
	barcode=GGAATAAGTTTGACTG-3
	barcode=GGAATAATCTTCATGT-3
	barcode=GGACAAGAGATATACG-3
	barcode=GGACAAGAGTCGAGTG-3
	barcode=GGACAAGCAACACCCG-3
	barcode=GGACAAGCAAGCTGTT-3
	barcode=GGACAAGCAGCTGTTA-3
	barcode=GGACAAGTCAACTCTT-3
	barcode=GGACAGACACTATCTT-3
	...

scsearch --file files/1M_neurons_filtered_gene_bc_matrices_h5.h5 ENSMUSG00000095742
Search index 10x for genes [ENSMUSG00000095742]
Matching cells: 592

scsearch --file files/1M_neurons_filtered_gene_bc_matrices_h5.h5 ENSMUSG00000050708 ENSMUSG00000095742
Search index 10x for genes [ENSMUSG00000050708, ENSMUSG00000095742]
Matching cells: 591
```

Delete an index with the following command:

```bash
scsearch -o delete --index 10x
```

[Install Elasticsearch]: https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html
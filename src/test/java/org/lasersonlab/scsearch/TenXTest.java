package org.lasersonlab.scsearch;

import org.junit.Test;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TenXTest {
    private static final String FILE = "files/1M_neurons_filtered_gene_bc_matrices_h5.h5";

    @Test
    public void testGetGeneNames() throws IOException {
        List<String> geneNames = TenX.getGeneNames(FILE);
        assertEquals(27998, geneNames.size());
    }

    @Test
    public void testReadShard() throws IOException, InvalidRangeException {
        int totalShards = 320;
        int numShards = 3;
        for (int i = 0; i < numShards; i++) {
            List<TenX.Cell> cells = TenX.readShard(FILE, totalShards, i);
            assertFalse(cells.isEmpty());
            List<String> geneNames = TenX.getGeneNames(FILE);
            List<String> topGenes = TenX.topGenes(cells, geneNames);
            assertFalse(topGenes.isEmpty());
            System.out.println(topGenes.subList(0, 50));
            System.out.println(topGenes.get(topGenes.size() - 1));
        }
    }
}

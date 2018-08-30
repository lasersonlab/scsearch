package org.lasersonlab.scsearch;

import org.junit.Test;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TenXTest {

    @Test
    public void testGetGeneNames() throws IOException, InvalidRangeException {
        String file = "/Users/tom/workspace/hdf5-java-cloud/files/1M_neurons_filtered_gene_bc_matrices_h5.h5";
        List<String> geneNames = TenX.getGeneNames(file);
        assertEquals(27998, geneNames.size());
    }

    @Test
    public void testReadShard() throws IOException, InvalidRangeException {
        String file = "/Users/tom/workspace/hdf5-java-cloud/files/1M_neurons_filtered_gene_bc_matrices_h5.h5";
        int totalShards = 320;
        int numShards = 3;
        for (int i = 0; i < numShards; i++) {
            List<TenX.Cell> cells = TenX.readShard(file, totalShards, i);
            assertFalse(cells.isEmpty());
        }
    }
}

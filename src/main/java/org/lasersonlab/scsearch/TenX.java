package org.lasersonlab.scsearch;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Convenience methods for reading a 10X dataset.
 */
public class TenX {

    static class Cell {
        public String barcode;
        public int[] geneIndices;

        public Cell(String barcode, int[] geneIndices) {
            this.barcode = barcode;
            this.geneIndices = geneIndices;
        }

        public String toJson() {
            // trim barcode since it has two strange characters at the end
            String trimmedBarcode = barcode.substring(0, barcode.length() - 2);
            return String.format("{\"barcode\":\"%s\",\"genes\":%s}", trimmedBarcode, Arrays.toString(geneIndices));
        }
    }

    public static List<String> getGeneNames(String file) throws IOException {
        try (NetcdfFile ncfile = NetcdfFile.open(file)) {
            Variable genes = ncfile.findVariable("/mm10/genes");
            String[] names = Arrays.stream((char[][]) genes.read().copyToNDJavaArray())
                    .map(String::new)
                    .toArray(String[]::new);
            return Arrays.asList(names);
        }
    }

    public static List<Cell> readShard(String file, int totalShards, int k) throws IOException, InvalidRangeException {
        try (NetcdfFile ncfile = NetcdfFile.open(file)) {
            Variable indptr = ncfile.findVariable("/mm10/indptr");
            Variable indices = ncfile.findVariable("/mm10/indices");
            Variable data = ncfile.findVariable("/mm10/data");
            Variable barcodes = ncfile.findVariable("/mm10/barcodes");
            Variable shape = ncfile.findVariable("/mm10/shape");

            int numFeatures = shape.read().getInt(0);
            int numRows = barcodes.getShape(0);

            int start = k * numRows / (totalShards - 1);
            int end;
            if (k == (totalShards - 1)) {
                end = numRows;
            } else {
                end = (k + 1) * numRows / (totalShards - 1);
            }

            String[] barcodeData = Arrays.stream(
                    ((char[][]) ArrayUtils.index(barcodes, start, end + 1).copyToNDJavaArray()))
                    .map(String::new)
                    .toArray(String[]::new);
            long[] indptrData = (long[]) ArrayUtils.index(indptr, start, end + 1).getStorage();
            long firstIndptr = indptrData[0];
            long lastIndptr = indptrData[indptrData.length - 1];
            if (firstIndptr == lastIndptr) {
                return Collections.emptyList();
            }
            long[] indicesData = (long[]) ArrayUtils.index(indices, firstIndptr, lastIndptr).getStorage();
            int[] dataData = (int[]) ArrayUtils.index(data, firstIndptr, lastIndptr).getStorage();

            List<Cell> cells = new ArrayList<>();
            for (int i = 0; i < end - start; i++) {
                String barcode = barcodeData[i];
                int startIndptr = (int) (indptrData[i] - firstIndptr);
                int endIndptr = (int) (indptrData[i + 1] - firstIndptr);
                int[] geneIndices = Arrays.stream(Arrays.copyOfRange(indicesData, startIndptr, endIndptr)).mapToInt(l -> (int) l).toArray();
                // note that we don't need the actual expression levels since we are indexing genes that have *any* level expressed
                cells.add(new Cell(barcode, geneIndices));
            }
            return cells;
        }
    }

    public static List<String> topGenes(List<Cell> cells, List<String> geneNames) {
        Map<String, Integer> counts = new HashMap<>();
        for (Cell cell : cells) {
            for (int geneIndex: cell.geneIndices) {
                String gene = geneNames.get(geneIndex);
                Integer count = counts.get(gene);
                if (count == null) {
                    count = 1;
                } else {
                    count = count + 1;
                }
                counts.put(gene, count);
            }
        }
        return counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .map(e -> e.getKey())
                .collect(Collectors.toList());
    }
}

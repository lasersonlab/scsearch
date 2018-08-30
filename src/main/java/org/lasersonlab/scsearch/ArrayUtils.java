package org.lasersonlab.scsearch;

import ucar.ma2.Array;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.ma2.Section;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Convenience methods to make it easier to work with
 * <a href="https://www.unidata.ucar.edu/software/thredds/current/netcdf-java/documentation.htm">netCDF</a>
 * {@link Array} objects.
 */
public class ArrayUtils {

  /**
   * The equivalent of <code>v[i]</code> in numpy.
   */
  public static Array index(Variable v, long i) throws InvalidRangeException, IOException {
    return index(v, i, i + 1);
  }

  /**
   * The equivalent of <code>v[i:j]</code> in numpy.
   */
  public static Array index(Variable v, long i, long j) throws InvalidRangeException, IOException {
    return index(v, 0, i, j);
  }

  /**
   * The equivalent of <code>v[i:j]</code> in numpy along a single dimension.
   */
  public static Array index(Variable v, int dimIndex, long i, long j) throws InvalidRangeException, IOException {
    int rank = v.getShape().length;
    List<Range> rs = new ArrayList<>();
    for (int r = 0; r < rank; r++) {
      if (r == dimIndex) {
        rs.add(new Range(i, j - 1));  // subtract 1 since Range end point is inclusive, unlike numpy
      } else {
        rs.add(new Range(v.getShape(r)));
      }
    }
    Section s = new Section(rs);
    return v.read(s);
  }

}

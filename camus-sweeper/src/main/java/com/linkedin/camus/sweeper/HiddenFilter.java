package com.linkedin.camus.sweeper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Filter a path if the path name starts with '_' or '.'
 */
public class HiddenFilter implements PathFilter {
  @Override
  public boolean accept(Path path) {
    String name = path.getName();
    return !(name.startsWith("_") || name.startsWith("."));
  }
}


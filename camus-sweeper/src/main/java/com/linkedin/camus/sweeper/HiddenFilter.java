package com.linkedin.camus.sweeper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class HiddenFilter implements PathFilter {
  @Override
  public boolean accept(Path path) {
    String name = path.getName();
    if (name.startsWith("_") || name.startsWith(".")) {
      return false;
    }

    return true;
  }
}


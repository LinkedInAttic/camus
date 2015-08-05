package com.linkedin.camus.sweeper.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;


public class Utils {

  /**
   * Scans the properties to find the keys starting with the provided prefix
   * Key-Values are returned as a map
   * 
   * @param props
   * @param prefix
   * @return
   */
  public static Map<String, String> getMapByPrefix(Properties props, String prefix) {
    HashMap<String, String> map = new HashMap<String, String>();
    for (Entry<Object, Object> pair : props.entrySet()) {
      String keyString = (String) pair.getKey();
      if (keyString.startsWith(prefix)) {
        map.put(keyString, (String) pair.getValue());
      }
    }
    return map;
  }

  /**
   * Splits the comma separated value for a key and returns a list containing
   * those strings
   * 
   * @param props
   * @param property
   * @return null if property doesnt exist, List of Strings otherwise
   */
  public static List<String> getStringList(Properties props, String property) {
    String value = (String) props.getProperty(property, null);
    if (value == null || value.isEmpty())
      return new ArrayList<String>();
    else
      return Arrays.asList(value.split(","));
  }

}

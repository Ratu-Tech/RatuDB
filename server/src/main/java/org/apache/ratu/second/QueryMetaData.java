/*
 * Copyright 2019 Genesys Telecommunications Laboratories, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratu.second;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds query meta data
 * <p>
 * Created by Jacques-Henri Berthemet on 01/06/2016.
 */
public class QueryMetaData {

  //syntax is #options:opt1=val1;opt2=val2#CQL;
  //; and # are not allowed in options
  private static final String META_PREFIX = "#options:";
  private static final String META_SUFFIX = "#";
  private static final String OPTION_SEPARATOR = ",";
  private static final String VALUE_SEPARATOR = "=";

  //Known options
  private static final String LOAD_ROWS = "load-rows";
  private static final String LOAD_SOURCE = "load-source";

  public final String query;
  private final Map<String, String> options = new HashMap<>();

  public QueryMetaData(@Nonnull String queryStr) {
    if (queryStr.startsWith(META_PREFIX)) {
      int endIndex = queryStr.indexOf(META_SUFFIX, META_PREFIX.length());
      query = queryStr.substring(endIndex + 1, queryStr.length());

      String optionStr = queryStr.substring(META_PREFIX.length(), endIndex);

      for (String option : optionStr.split(OPTION_SEPARATOR)) {
        String[] items = option.split(VALUE_SEPARATOR);
        options.put(items[0], items[1]);
      }

    } else {
      query = queryStr;
    }
  }

  /**
   * @return (true default) load rows from Cassandra
   */
  public boolean loadRows() {
    String value = options.get(LOAD_ROWS);
    return value == null ? true : Boolean.valueOf(value);
  }

  /**
   * @return (false default) return _source when loading data from ES
   */
  public boolean loadSource() {
    String value = options.get(LOAD_SOURCE);
    return value == null ? false : Boolean.valueOf(value);
  }
}

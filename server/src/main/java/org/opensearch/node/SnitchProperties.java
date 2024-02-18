/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.env.Environment;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

public class SnitchProperties {
    final Logger logger = LogManager.getLogger(SnitchProperties.class);

    public static final String RACKDC_PROPERTY_FILENAME = "cassandra-rackdc.properties";

    private final Properties properties;

    public SnitchProperties(Environment initialEnvironment) {
        properties = new Properties();
        InputStream stream = null;
        String configURL = initialEnvironment.configDir().toString() + "/"+RACKDC_PROPERTY_FILENAME;
        try {
            URL url=new URL("file://"+configURL);
            stream = url.openStream(); // catch block handles potential NPE
            properties.load(stream);
        } catch (Exception e) {
            // do not throw exception here, just consider this an incomplete or an empty property file.
            logger.warn("Unable to read {}", ((configURL != null) ? configURL : RACKDC_PROPERTY_FILENAME));
        } finally {
            try {
                if (stream != null)
                    stream.close();
            } catch (Exception e) {
                logger.warn("Failed closing {}", stream, e);
            }
        }
    }

    /**
     * Get a snitch property value or return defaultValue if not defined.
     */
    public String get(String propertyName) {
        return properties.getProperty(propertyName);
    }

    public boolean contains(String propertyName) {
        return properties.containsKey(propertyName);
    }
}

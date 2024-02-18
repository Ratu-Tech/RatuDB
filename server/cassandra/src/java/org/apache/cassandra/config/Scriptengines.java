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

package org.apache.cassandra.config;

import org.apache.cassandra.utils.Shared;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public class Scriptengines {

    public String gremlin_groovy_plugins;

    public Map<String,Object> plugins;

    public Scriptengines(){}

    public Scriptengines(String gremlin_groovy_plugins){
        this.gremlin_groovy_plugins = gremlin_groovy_plugins;
    }

    public void setPlugins(){
        if (!StringUtils.isBlank(gremlin_groovy_plugins)){

        }
    }


}

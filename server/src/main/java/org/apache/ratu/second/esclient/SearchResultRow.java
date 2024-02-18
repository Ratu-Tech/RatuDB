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

package org.apache.ratu.second.esclient;

import com.alibaba.fastjson2.JSONObject;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class SearchResultRow {

    public final String[] primaryKey;

    public final JSONObject docMetadata;

    public ByteBuffer partitionKey;

    public String[] clusteringKeys;

    public SearchResultRow(@Nonnull String[] primaryKey,@Nonnull JSONObject docMetadata){
        this.primaryKey=primaryKey;
        this.docMetadata=docMetadata;
    }

}

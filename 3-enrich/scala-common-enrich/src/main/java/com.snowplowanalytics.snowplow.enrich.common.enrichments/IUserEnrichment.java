/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common.enrichments;

import java.util.ArrayList;
import com.fasterxml.jackson.databind.JsonNode;
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent;

public interface IUserEnrichment {

    public void configure(JsonNode config) throws Exception;

    public ArrayList<JsonNode> createDerivedContexts(
        EnrichedEvent event,
        JsonNode unstructEvent,
        ArrayList<JsonNode> existingContexts) throws Exception;
}

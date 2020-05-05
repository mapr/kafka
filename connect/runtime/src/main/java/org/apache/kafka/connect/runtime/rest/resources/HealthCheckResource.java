/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.streams.mapr.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/healthz")
@Produces(MediaType.APPLICATION_JSON)
public class HealthCheckResource {
    private static final Logger log = LoggerFactory.getLogger(HealthCheckResource.class);

    private Set<String> streams;

    public HealthCheckResource(WorkerConfig config) {
        Set<String> topics = new HashSet<>();
        try {
            topics.add(config.getString(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG));
            topics.add(config.getString(DistributedConfig.CONFIG_TOPIC_CONFIG));
            topics.add(config.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG));
            streams = topics.stream().map(x -> x.substring(0, x.indexOf(':'))).collect(Collectors.toSet());
        } catch (Exception e) {
            streams = null;
            log.error("Couldn't get internal streams from topics: " + e.getMessage());
        }
    }

    @GET
    public Response healthCheck() {
        try {
            if (streams == null) {
                throw new Exception("Couldn't get internal streams");
            }
            boolean anyStreamNotExists = streams.stream().map(Utils::streamExists).anyMatch(x -> !x);
            if (anyStreamNotExists) {
                throw new Exception("Internal stream doesn't exist");
            }
        } catch(Exception e) {
            log.error("Health check failed: " + e.getMessage());
            throw new InternalServerErrorException();
        }
        return Response.ok().build();
    }
}

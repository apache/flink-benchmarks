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

package org.apache.flink.benchmark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.util.FlinkRuntimeException;

import java.net.URI;
import java.time.Duration;
import java.util.List;

/** Utility class for querying a backpressure status. */
public class BackpressureUtils {

    static void waitForBackpressure(
            JobID jobID,
            List<JobVertexID> sourceId,
            URI restAddress,
            Configuration clientConfiguration)
            throws Exception {
        RestClusterClient<StandaloneClusterId> restClient =
                createClient(restAddress.getPort(), clientConfiguration);
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(60));
        boolean allBackpressured;
        // There seems to be a race condition in some setups, between setting up REST server
        // and client being able to connect. This is handled by the retrying mechanism in
        // the RestClusterClient, but time out takes a lot of time to trigger, so we are
        // doing a little bit of sleep here in an attempt to avoid waiting for that timeout.
        Thread.sleep(100);
        do {
            allBackpressured =
                    sourceId.stream()
                            .map(id -> queryBackpressure(jobID, id, restClient, restAddress))
                            .allMatch(
                                    level ->
                                            level
                                                    == JobVertexBackPressureInfo
                                                            .VertexBackPressureLevel.HIGH);
        } while (!allBackpressured && deadline.hasTimeLeft());
        if (!allBackpressured) {
            throw new FlinkRuntimeException(
                    "Could not trigger backpressure for the job in given time.");
        }
    }

    private static RestClusterClient<StandaloneClusterId> createClient(
            int port, Configuration clientConfiguration) throws Exception {
        final Configuration clientConfig = new Configuration();
        clientConfig.addAll(clientConfiguration);
        clientConfig.setInteger(RestOptions.PORT, port);
        return new RestClusterClient<>(clientConfig, StandaloneClusterId.getInstance());
    }

    private static JobVertexBackPressureInfo.VertexBackPressureLevel queryBackpressure(
            JobID jobID, JobVertexID vertexID, RestClusterClient restClient, URI restAddress) {
        try {
            final JobVertexMessageParameters metricsParameters = new JobVertexMessageParameters();
            metricsParameters.jobPathParameter.resolve(jobID);
            metricsParameters.jobVertexIdPathParameter.resolve(vertexID);
            return ((JobVertexBackPressureInfo)
                            restClient
                                    .sendRequest(
                                            JobVertexBackPressureHeaders.getInstance(),
                                            metricsParameters,
                                            EmptyRequestBody.getInstance())
                                    .get())
                    .getBackpressureLevel();
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }
}

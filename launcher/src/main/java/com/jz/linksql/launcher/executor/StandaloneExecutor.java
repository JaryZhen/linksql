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

package com.jz.linksql.launcher.executor;

import com.jz.linksql.core.Main;
import com.jz.linksql.core.enums.EPluginLoadMode;
import com.jz.linksql.launcher.entity.JobParamsInfo;
import com.jz.linksql.launcher.factory.StandaloneClientFactory;
import com.jz.linksql.launcher.utils.JobGraphBuildUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;

/**
 * Date: 2021/3/6
 *
 * @author Jaryzhen
 */
public class StandaloneExecutor {
    JobParamsInfo jobParamsInfo;

    public StandaloneExecutor(JobParamsInfo jobParamsInfo) {
        this.jobParamsInfo = jobParamsInfo;
    }

    public void exec() throws Exception {

        Preconditions.checkArgument(StringUtils.equalsIgnoreCase(jobParamsInfo.getPluginLoadMode(), EPluginLoadMode.CLASSPATH.name()),
                "standalone only supports classpath mode");

        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo);
        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir(), jobParamsInfo.getConfProperties());

        if (!StringUtils.isBlank(jobParamsInfo.getUdfJar())) {
            JobGraphBuildUtil.fillUserJarForJobGraph(jobParamsInfo.getUdfJar(), jobGraph);
        }

        JobGraphBuildUtil.fillJobGraphClassPath(jobGraph);

        ClusterDescriptor clusterDescriptor = StandaloneClientFactory.INSTANCE.createClusterDescriptor("", flinkConfiguration);
        ClusterClientProvider clusterClientProvider = clusterDescriptor.retrieve(StandaloneClusterId.getInstance());
        ClusterClient clusterClient = clusterClientProvider.getClusterClient();

  /*      JobExecutionResult jobExecutionResult = ClientUtils.submitJob(clusterClient, jobGraph);

        PackagedProgram prg =
                PackagedProgram.newBuilder()
                        .setEntryPointClassName(Main.class.getName())
                        .build();

        ClientUtils.executeProgram(
                new TestExecutorServiceLoader(clusterClient, plan),
                flinkConfiguration,
                prg,
                false,
                false);
        String jobId = jobExecutionResult.getJobID().toString();
        System.out.println("jobID:" + jobId);
*/
    }


}

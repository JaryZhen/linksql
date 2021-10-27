package com.jz.linksql.launcher.executor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @Author: Jary
 * @Date: 2021/10/27 1:40 下午
 */
public class TestExecutorServiceLoader implements PipelineExecutorServiceLoader {
    private static final String TEST_EXECUTOR_NAME = "test_executor";


    private final ClusterClient<?> clusterClient;

    private final Plan plan;

    TestExecutorServiceLoader(final ClusterClient<?> clusterClient, final Plan plan) {
        this.clusterClient = checkNotNull(clusterClient);
        this.plan = checkNotNull(plan);
    }

    @Override
    public PipelineExecutorFactory getExecutorFactory(@Nonnull Configuration configuration) {
        return new PipelineExecutorFactory() {

            @Override
            public String getName() {
                return "my-name";
            }

            @Override
            public boolean isCompatibleWith(@Nonnull Configuration configuration) {
                return TEST_EXECUTOR_NAME.equalsIgnoreCase(
                        configuration.getString(DeploymentOptions.TARGET));
            }

            @Override
            public PipelineExecutor getExecutor(@Nonnull Configuration configuration) {
                return (pipeline, config, classLoader) -> {
                    final int parallelism = config.getInteger(CoreOptions.DEFAULT_PARALLELISM);
                    final JobGraph jobGraph =
                            FlinkPipelineTranslationUtil.getJobGraph(plan, config, parallelism);

                    final ExecutionConfigAccessor accessor =
                            ExecutionConfigAccessor.fromConfiguration(config);
                    jobGraph.addJars(accessor.getJars());
                    jobGraph.setClasspaths(accessor.getClasspaths());

                    final JobID jobID = clusterClient.submitJob(jobGraph).get();
                    return CompletableFuture.completedFuture(
                            new ClusterClientJobClientAdapter<>(
                                    () -> clusterClient, jobID, classLoader));
                };
            }
        };
    }

    @Override
    public Stream<String> getExecutorNames() {
        throw new UnsupportedOperationException("not implemented");
    }
}

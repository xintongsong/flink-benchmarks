package org.apache.flink.benchmark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {
    public final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

    private final int parallelism = 1;
    private final boolean objectReuse = true;

    @Setup
    public void setUp() throws IOException {
        // set up the execution environment
        env.setParallelism(parallelism);
        env.getConfig().disableSysoutLogging();
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

        env.setStateBackend(new MemoryStateBackend());
    }

    public void execute() throws Exception {
        env.execute();
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment() {
        final Configuration configuration = new Configuration();
        configuration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, 32768);
        return StreamExecutionEnvironment.createLocalEnvironment(1, configuration);
    }
}

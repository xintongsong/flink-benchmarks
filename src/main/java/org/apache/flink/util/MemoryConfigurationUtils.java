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

package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;

/**
 * Utils for memory configuration for the benchmarks.
 */
public class MemoryConfigurationUtils {

    /**
     * Get a configuration that has the same amount of network buffers as before FLIP-49.
     */
    public static Configuration getConfigurationWithLegacyNetworkBuffers() {
        return adjustConfigurationWithLegacyNetworkBuffers(new Configuration());
    }

    /**
     * Adjusts a give configuration to have the same amount of network buffers as before FLIP-49.
     */
    @SuppressWarnings("deprecation")
    public static Configuration adjustConfigurationWithLegacyNetworkBuffers(final Configuration configuration) {
        final Configuration modifiedConfiguration = new Configuration(configuration);
        modifiedConfiguration.setInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, deriveLegacyNumNetworkBuffers(configuration));
        return modifiedConfiguration;
    }

    private static int deriveLegacyNumNetworkBuffers(final Configuration configuration) {
        if (!hasNewNetworkConfig(configuration)) {
            // fallback: number of network buffers
            @SuppressWarnings("deprecation")
            final int numberOfNetworkBuffers = configuration.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
            checkOldNetworkConfig(numberOfNetworkBuffers);
            return numberOfNetworkBuffers;
        } else {
            final long maxJvmHeapMemory = EnvironmentInformation.getMaxJvmHeapMemory();
            final long networkMemorySize = deriveLegacyNetworkBufferMemoryFromJvmHeapSize(configuration, maxJvmHeapMemory);
            // tolerate offcuts between intended and allocated memory due to segmentation (will be available to the user-space memory)
            long numberOfNetworkBuffersLong = networkMemorySize / ConfigurationParserUtils.getPageSize(configuration);
            if (numberOfNetworkBuffersLong > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("The given number of memory bytes (" + networkMemorySize
                    + ") corresponds to more than MAX_INT pages.");
            }
            return (int) numberOfNetworkBuffersLong;
        }
    }

    public static long deriveLegacyNetworkBufferMemoryFromJvmHeapSize(Configuration config, long maxJvmHeapMemory) {
        // assuming managed memory is always on-heap, to align with the legacy default behavior
        final long heapAndManagedMemory = maxJvmHeapMemory;

        // finally extract the network buffer memory size again from:
        // heapAndManagedMemory = totalProcessMemory - networkReservedMemory
        //                      = totalProcessMemory - Math.min(networkBufMax, Math.max(networkBufMin, totalProcessMemory * networkBufFraction)
        // totalProcessMemory = heapAndManagedMemory / (1.0 - networkBufFraction)
        float networkBufFraction = config.getFloat(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION);

        // Converts to double for higher precision. Converting via string achieve higher precision for those
        // numbers can not be represented preciously by float, like 0.4f.
        double heapAndManagedFraction = 1.0 - Double.valueOf(Float.toString(networkBufFraction));
        long totalProcessMemory = (long) (heapAndManagedMemory / heapAndManagedFraction);
        long networkMemoryByFraction = (long) (totalProcessMemory * networkBufFraction);

        return alignNetworkMemoryToConfiguredRange(config, networkMemoryByFraction);
    }

    private static long alignNetworkMemoryToConfiguredRange(Configuration config, long networkMemoryByFraction) {
        float networkBufFraction = config.getFloat(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION);
        long networkBufMin = MemorySize.parse(config.getString(TaskManagerOptions.SHUFFLE_MEMORY_MIN)).getBytes();
        long networkBufMax = MemorySize.parse(config.getString(TaskManagerOptions.SHUFFLE_MEMORY_MAX)).getBytes();

        int pageSize = ConfigurationParserUtils.getPageSize(config);

        checkNewNetworkConfig(pageSize, networkBufFraction, networkBufMin, networkBufMax);

        long networkBufBytes = Math.min(networkBufMax, Math.max(networkBufMin, networkMemoryByFraction));

        return networkBufBytes;
    }

    @SuppressWarnings("deprecation")
    private static void checkOldNetworkConfig(final int numNetworkBuffers) {
        ConfigurationParserUtils.checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
            NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS.key(),
            "Must have at least one network buffer");
    }

    private static void checkNewNetworkConfig(
        final int pageSize,
        final float networkBufFraction,
        final long networkBufMin,
        final long networkBufMax) throws IllegalConfigurationException {

        ConfigurationParserUtils.checkConfigParameter(networkBufFraction > 0.0f && networkBufFraction < 1.0f, networkBufFraction,
            TaskManagerOptions.SHUFFLE_MEMORY_FRACTION.key(),
            "Network buffer memory fraction of the free memory must be between 0.0 and 1.0");

        ConfigurationParserUtils.checkConfigParameter(networkBufMin >= pageSize, networkBufMin,
            TaskManagerOptions.SHUFFLE_MEMORY_MIN.key(),
            "Minimum memory for network buffers must allow at least one network " +
                "buffer with respect to the memory segment size");

        ConfigurationParserUtils.checkConfigParameter(networkBufMax >= pageSize, networkBufMax,
            TaskManagerOptions.SHUFFLE_MEMORY_MAX.key(),
            "Maximum memory for network buffers must allow at least one network " +
                "buffer with respect to the memory segment size");

        ConfigurationParserUtils.checkConfigParameter(networkBufMax >= networkBufMin, networkBufMax,
            TaskManagerOptions.SHUFFLE_MEMORY_MAX.key(),
            "Maximum memory for network buffers must not be smaller than minimum memory (" +
                TaskManagerOptions.SHUFFLE_MEMORY_MIN.key() + ": " + networkBufMin + ")");
    }

    @SuppressWarnings("deprecation")
    private static boolean hasNewNetworkConfig(final Configuration config) {
        return config.contains(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION) ||
            config.contains(TaskManagerOptions.SHUFFLE_MEMORY_MIN) ||
            config.contains(TaskManagerOptions.SHUFFLE_MEMORY_MAX) ||
            !config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
    }
}

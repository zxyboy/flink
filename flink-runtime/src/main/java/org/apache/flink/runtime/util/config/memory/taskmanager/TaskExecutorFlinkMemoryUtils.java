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

package org.apache.flink.runtime.util.config.memory.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.config.memory.FlinkMemoryUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.RangeFraction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkMemoryUtils} for Task Executor.
 *
 * <p>The required fine-grained components are {@link TaskManagerOptions#TASK_HEAP_MEMORY} and
 * {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}.
 */
public class TaskExecutorFlinkMemoryUtils implements FlinkMemoryUtils<TaskExecutorFlinkMemory> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorFlinkMemoryUtils.class);

    @Override
    public TaskExecutorFlinkMemory deriveFromRequiredFineGrainedOptions(Configuration config) {
        // 任务堆内存： taskmanager.memory.task.heap.size
        final MemorySize taskHeapMemorySize = getTaskHeapMemorySize(config);
        // 管理内存: taskmanager.memory.managed.size
        final MemorySize managedMemorySize = getManagedMemorySize(config);
        // 框架堆内存： taskmanager.memory.framework.heap.size
        final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
        // 框架堆外内存： taskmanager.memory.framework.off-heap.size
        final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
        // 任务堆外内存： taskmanager.memory.task.off-heap.size
        final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

        final MemorySize networkMemorySize;
        final MemorySize totalFlinkExcludeNetworkMemorySize =
                frameworkHeapMemorySize
                        .add(frameworkOffHeapMemorySize)
                        .add(taskHeapMemorySize)
                        .add(taskOffHeapMemorySize)
                        .add(managedMemorySize);
        /// 如果配置了总内存大小，则从总内存大小中推断出网络内存大小
        if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
            // derive network memory from total flink memory, and check against network min/max
            final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
            if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Heap Memory ("
                                + taskHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + ") and Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            // 推断网络内存大小 : totalFlinkMemorySize - totalFlinkExcludeNetworkMemorySize
            networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
            sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
                    config, networkMemorySize, totalFlinkMemorySize);
        } else {
            // derive network memory from network configs

            networkMemorySize =
                    isUsingLegacyNetworkConfigs(config)
                            ? getNetworkMemorySizeWithLegacyConfig(config)
                            : deriveNetworkMemoryWithInverseFraction(
                                    config, totalFlinkExcludeNetworkMemorySize);
        }

        final TaskExecutorFlinkMemory flinkInternalMemory =
                new TaskExecutorFlinkMemory(
                        frameworkHeapMemorySize,
                        frameworkOffHeapMemorySize,
                        taskHeapMemorySize,
                        taskOffHeapMemorySize,
                        networkMemorySize,
                        managedMemorySize);
        sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

        return flinkInternalMemory;
    }

    @Override
    public TaskExecutorFlinkMemory deriveFromTotalFlinkMemory(
            final Configuration config, final MemorySize totalFlinkMemorySize) {
        // 获取flink框架堆内存： taskmanager.memory.framework.heap.size， 默认：128m
        final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
        // 获取flink框架非堆内存： taskmanager.memory.framework.off-heap.size， 默认：128m
        final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
        // 获取task非堆内存： taskmanager.memory.task.off-heap.size， 默认：0
        final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

        // 推断task堆内存： taskmanager.memory.task.heap.size
        final MemorySize taskHeapMemorySize;
        // 推断network memory： taskmanager.memory.network.min/max/fraction
        final MemorySize networkMemorySize;
        // 推断managed memory： taskmanager.memory.managed.size
        final MemorySize managedMemorySize;

        // 是否显示配置： taskmanager.memory.task.heap.size
        if (isTaskHeapMemorySizeExplicitlyConfigured(config)) {
            // task heap memory is configured,
            // derive managed memory first, leave the remaining to network memory and check against
            // network min/max
            // 获取配置的task堆内存： taskmanager.memory.task.heap.size
            taskHeapMemorySize = getTaskHeapMemorySize(config);
            // 推断managed memory： taskmanager.memory.managed.size,
            // 如果配置了managed memory, 则使用配置的managed memory， 否则使用：flink总内存 * managed memory fraction (默认： 0.4)
            managedMemorySize =
                    deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
            final MemorySize totalFlinkExcludeNetworkMemorySize =
                    frameworkHeapMemorySize
                            .add(frameworkOffHeapMemorySize)
                            .add(taskHeapMemorySize)
                            .add(taskOffHeapMemorySize)
                            .add(managedMemorySize);
            // 除去network memory后的总内存 > flink总内存， 则抛出异常
            if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Heap Memory ("
                                + taskHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + ") and Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            // 推断network memory： 总Flink内存 - 除去network memory后的总内存
            networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
            // 检查推断的network memory是否合法
            sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
                    config, networkMemorySize, totalFlinkMemorySize);
        } else {
            // task heap memory is not configured
            // derive managed memory and network memory, leave the remaining to task heap memory
            // 推断managed memory：如果显示配置了managed memory，则直接获取配置的managed memory,否则，根据配置的managed memory fraction推断managed memory
            managedMemorySize =
                    deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
            // 推测network memory：如果使用的是旧的配置，则直接获取配置的network memory,否则，根据配置的network memory fraction推断network memory
            networkMemorySize =
                    isUsingLegacyNetworkConfigs(config)
                            ? getNetworkMemorySizeWithLegacyConfig(config)
                            : deriveNetworkMemoryWithFraction(config, totalFlinkMemorySize);
            final MemorySize totalFlinkExcludeTaskHeapMemorySize =
                    frameworkHeapMemorySize
                            .add(frameworkOffHeapMemorySize)
                            .add(taskOffHeapMemorySize)
                            .add(managedMemorySize)
                            .add(networkMemorySize);
            // 除去task heap memory后的总内存 > flink总内存， 则抛出异常
            if (totalFlinkExcludeTaskHeapMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + "), Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") and Network Memory ("
                                + networkMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            // 推断task heap memory： 总Flink内存 - 除去task heap memory后的总内存
            taskHeapMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeTaskHeapMemorySize);
        }

        final TaskExecutorFlinkMemory flinkInternalMemory =
                new TaskExecutorFlinkMemory(
                        frameworkHeapMemorySize,
                        frameworkOffHeapMemorySize,
                        taskHeapMemorySize,
                        taskOffHeapMemorySize,
                        networkMemorySize,
                        managedMemorySize);
        sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

        return flinkInternalMemory;
    }

    private static MemorySize deriveManagedMemoryAbsoluteOrWithFraction(
            final Configuration config, final MemorySize base) {
        // 如果显示配置了managed memory，则直接获取配置的managed memory
        return isManagedMemorySizeExplicitlyConfigured(config)
                ? getManagedMemorySize(config)
                // 否则，根据配置的managed memory fraction推断managed memory
                : ProcessMemoryUtils.deriveWithFraction(
                        "managed memory", base, getManagedMemoryRangeFraction(config));
    }

    private static MemorySize deriveNetworkMemoryWithFraction(
            final Configuration config, final MemorySize base) {
        return ProcessMemoryUtils.deriveWithFraction(
                "network memory", base, getNetworkMemoryRangeFraction(config));
    }

    private static MemorySize deriveNetworkMemoryWithInverseFraction(
            final Configuration config, final MemorySize base) {
        return ProcessMemoryUtils.deriveWithInverseFraction(
                "network memory", base, getNetworkMemoryRangeFraction(config));
    }

    public static MemorySize getFrameworkHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY);
    }

    public static MemorySize getFrameworkOffHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY);
    }

    private static MemorySize getTaskHeapMemorySize(final Configuration config) {
        checkArgument(isTaskHeapMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TASK_HEAP_MEMORY);
    }

    private static MemorySize getTaskOffHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TASK_OFF_HEAP_MEMORY);
    }

    private static MemorySize getManagedMemorySize(final Configuration config) {
        checkArgument(isManagedMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.MANAGED_MEMORY_SIZE);
    }

    private static RangeFraction getManagedMemoryRangeFraction(final Configuration config) {
        return ProcessMemoryUtils.getRangeFraction(
                MemorySize.ZERO,
                MemorySize.MAX_VALUE,
                TaskManagerOptions.MANAGED_MEMORY_FRACTION,
                config);
    }

    private static MemorySize getNetworkMemorySizeWithLegacyConfig(final Configuration config) {
        checkArgument(isUsingLegacyNetworkConfigs(config));
        @SuppressWarnings("deprecation")
        final long numOfBuffers =
                config.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
        final long pageSize = ConfigurationParserUtils.getPageSize(config);
        return new MemorySize(numOfBuffers * pageSize);
    }
    /// 获取网络内存的范围
    private static RangeFraction getNetworkMemoryRangeFraction(final Configuration config) {
        // 网络内存最小值： taskmanager.memory.network.min , 默认值： 64mb
        final MemorySize minSize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, TaskManagerOptions.NETWORK_MEMORY_MIN);
        // 网络内存最大值： taskmanager.memory.network.max : 默认值： 1gb
        final MemorySize maxSize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, TaskManagerOptions.NETWORK_MEMORY_MAX);
        return ProcessMemoryUtils.getRangeFraction(
                minSize, maxSize, TaskManagerOptions.NETWORK_MEMORY_FRACTION, config);
    }

    private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
        checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TOTAL_FLINK_MEMORY);
    }

    private static boolean isTaskHeapMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.TASK_HEAP_MEMORY);
    }

    private static boolean isManagedMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE);
    }

    private static boolean isUsingLegacyNetworkConfigs(final Configuration config) {
        // use the legacy number-of-buffer config option only when it is explicitly configured and
        // none of new config options is explicitly configured
        final boolean anyNetworkConfigured =
                config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN)
                        || config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX)
                        || config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
        final boolean legacyConfigured =
                config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
        return !anyNetworkConfigured && legacyConfigured;
    }

    private static boolean isNetworkMemoryFractionExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
    }

    private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY);
    }

    private static void sanityCheckTotalFlinkMemory(
            final Configuration config, final TaskExecutorFlinkMemory flinkInternalMemory) {
        if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
            final MemorySize configuredTotalFlinkMemorySize = getTotalFlinkMemorySize(config);
            if (!configuredTotalFlinkMemorySize.equals(
                    flinkInternalMemory.getTotalFlinkMemorySize())) {
                throw new IllegalConfigurationException(
                        "Configured and Derived Flink internal memory sizes (total "
                                + flinkInternalMemory
                                        .getTotalFlinkMemorySize()
                                        .toHumanReadableString()
                                + ") do not add up to the configured Total Flink Memory size ("
                                + configuredTotalFlinkMemorySize.toHumanReadableString()
                                + "). Configured and Derived Flink internal memory sizes are: "
                                + "Framework Heap Memory ("
                                + flinkInternalMemory.getFrameworkHeap().toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + flinkInternalMemory.getFrameworkOffHeap().toHumanReadableString()
                                + "), Task Heap Memory ("
                                + flinkInternalMemory.getTaskHeap().toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + flinkInternalMemory.getTaskOffHeap().toHumanReadableString()
                                + "), Network Memory ("
                                + flinkInternalMemory.getNetwork().toHumanReadableString()
                                + "), Managed Memory ("
                                + flinkInternalMemory.getManaged().toHumanReadableString()
                                + ").");
            }
        }
    }
    // 验证网络内存
    private static void sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
            final Configuration config,
            final MemorySize derivedNetworkMemorySize,
            final MemorySize totalFlinkMemorySize) {
        try {
            sanityCheckNetworkMemory(config, derivedNetworkMemorySize, totalFlinkMemorySize);
        } catch (IllegalConfigurationException e) {
            throw new IllegalConfigurationException(
                    "If Total Flink, Task Heap and (or) Managed Memory sizes are explicitly configured then "
                            + "the Network Memory size is the rest of the Total Flink memory after subtracting all other "
                            + "configured types of memory, but the derived Network Memory is inconsistent with its configuration.",
                    e);
        }
    }
    // 验证网络内存
    private static void sanityCheckNetworkMemory(
            final Configuration config,
            final MemorySize derivedNetworkMemorySize,
            final MemorySize totalFlinkMemorySize) {
        // 传统配置： 已废弃，此处可以忽略。
        if (isUsingLegacyNetworkConfigs(config)) {
            final MemorySize configuredNetworkMemorySize =
                    getNetworkMemorySizeWithLegacyConfig(config);
            if (!configuredNetworkMemorySize.equals(derivedNetworkMemorySize)) {
                throw new IllegalConfigurationException(
                        "Derived Network Memory size ("
                                + derivedNetworkMemorySize.toHumanReadableString()
                                + ") does not match configured Network Memory size ("
                                + configuredNetworkMemorySize.toHumanReadableString()
                                + ").");
            }
        } else {
            // 网络内存的范围
            final RangeFraction networkRangeFraction = getNetworkMemoryRangeFraction(config);
            // 验证网络内存的范围
            // 如果网络内存大于网络内存的最大值，或者小于网络内存的最小值，抛出异常
            if (derivedNetworkMemorySize.getBytes() > networkRangeFraction.getMaxSize().getBytes()
                    || derivedNetworkMemorySize.getBytes()
                            < networkRangeFraction.getMinSize().getBytes()) {
                throw new IllegalConfigurationException(
                        "Derived Network Memory size ("
                                + derivedNetworkMemorySize.toHumanReadableString()
                                + ") is not in configured Network Memory range ["
                                + networkRangeFraction.getMinSize().toHumanReadableString()
                                + ", "
                                + networkRangeFraction.getMaxSize().toHumanReadableString()
                                + "].");
            }
            // 如果网络内存的大小被明确配置，并且不等于从配置的总Flink内存大小中派生的网络内存大小，则打印日志
            if (isNetworkMemoryFractionExplicitlyConfigured(config)
                    && !derivedNetworkMemorySize.equals(
                            totalFlinkMemorySize.multiply(networkRangeFraction.getFraction()))) {
                LOG.info(
                        "The derived Network Memory size ({}) does not match "
                                + "the configured Network Memory fraction ({}) from the configured Total Flink Memory size ({}). "
                                + "The derived Network Memory size will be used.",
                        derivedNetworkMemorySize.toHumanReadableString(),
                        networkRangeFraction.getFraction(),
                        totalFlinkMemorySize.toHumanReadableString());
            }
        }
    }
}

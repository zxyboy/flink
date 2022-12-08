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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.util.Preconditions;

import java.io.File;

/** Configuration object containing values for the rest handler configuration. */
public class RestHandlerConfiguration {

    private final long refreshInterval;

    private final int maxCheckpointStatisticCacheEntries;

    private final Time timeout;

    private final File webUiDir;

    private final boolean webSubmitEnabled;

    private final boolean webCancelEnabled;

    public RestHandlerConfiguration(
            long refreshInterval,
            int maxCheckpointStatisticCacheEntries,
            Time timeout,
            File webUiDir,
            boolean webSubmitEnabled,
            boolean webCancelEnabled) {
        Preconditions.checkArgument(
                refreshInterval > 0L, "The refresh interval (ms) should be larger than 0.");
        this.refreshInterval = refreshInterval;

        this.maxCheckpointStatisticCacheEntries = maxCheckpointStatisticCacheEntries;

        this.timeout = Preconditions.checkNotNull(timeout);
        this.webUiDir = Preconditions.checkNotNull(webUiDir);
        this.webSubmitEnabled = webSubmitEnabled;
        this.webCancelEnabled = webCancelEnabled;
    }

    public long getRefreshInterval() {
        return refreshInterval;
    }

    public int getMaxCheckpointStatisticCacheEntries() {
        return maxCheckpointStatisticCacheEntries;
    }

    public Time getTimeout() {
        return timeout;
    }

    public File getWebUiDir() {
        return webUiDir;
    }

    public boolean isWebSubmitEnabled() {
        return webSubmitEnabled;
    }

    public boolean isWebCancelEnabled() {
        return webCancelEnabled;
    }

    public static RestHandlerConfiguration fromConfiguration(Configuration configuration) {
        // web页面刷新时间：web.refresh-interval， 默认：3s
        final long refreshInterval = configuration.getLong(WebOptions.REFRESH_INTERVAL);
        // 最大checkpoint数量：web.checkpoints.history，默认：10
        final int maxCheckpointStatisticCacheEntries =
                configuration.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);
        // web页面超时时间：web.timeout， 默认：10分钟
        final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

        final String rootDir = "flink-web-ui";
        // webui目录：web.tmpdir，默认：${java.io.tmpdir}/flink-web-ui
        final File webUiDir = new File(configuration.getString(WebOptions.TMP_DIR), rootDir);
        // web页面是否支持提交任务：web.submit.enable，默认：true
        final boolean webSubmitEnabled = configuration.getBoolean(WebOptions.SUBMIT_ENABLE);
        // web页面是否支持取消任务：web.cancel.enable，默认：true
        final boolean webCancelEnabled = configuration.getBoolean(WebOptions.CANCEL_ENABLE);

        return new RestHandlerConfiguration(
                refreshInterval,
                maxCheckpointStatisticCacheEntries,
                timeout,
                webUiDir,
                webSubmitEnabled,
                webCancelEnabled);
    }
}

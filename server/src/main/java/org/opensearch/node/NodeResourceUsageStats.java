/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * This represents the resource usage stats of a node along with the timestamp at which the stats object was created
 * in the respective node
 */
public class NodeResourceUsageStats implements Writeable {
    final String nodeId;
    long timestamp;
    double cpuUtilizationPercent;
    double memoryUtilizationPercent;

    public NodeResourceUsageStats(String nodeId, long timestamp, double memoryUtilizationPercent, double cpuUtilizationPercent) {
        this.nodeId = nodeId;
        this.timestamp = timestamp;
        this.cpuUtilizationPercent = cpuUtilizationPercent;
        this.memoryUtilizationPercent = memoryUtilizationPercent;
    }

    public NodeResourceUsageStats(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.timestamp = in.readLong();
        this.cpuUtilizationPercent = in.readDouble();
        this.memoryUtilizationPercent = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeLong(this.timestamp);
        out.writeDouble(this.cpuUtilizationPercent);
        out.writeDouble(this.memoryUtilizationPercent);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NodeResourceUsageStats[");
        sb.append(nodeId).append("](");
        sb.append("Timestamp: ").append(timestamp);
        sb.append(", CPU utilization percent: ").append(String.format(Locale.ROOT, "%.1f", cpuUtilizationPercent));
        sb.append(", Memory utilization percent: ").append(String.format(Locale.ROOT, "%.1f", memoryUtilizationPercent));
        sb.append(")");
        return sb.toString();
    }

    NodeResourceUsageStats(NodeResourceUsageStats nodeResourceUsageStats) {
        this(
            nodeResourceUsageStats.nodeId,
            nodeResourceUsageStats.timestamp,
            nodeResourceUsageStats.memoryUtilizationPercent,
            nodeResourceUsageStats.cpuUtilizationPercent
        );
    }

    public double getMemoryUtilizationPercent() {
        return memoryUtilizationPercent;
    }

    public double getCpuUtilizationPercent() {
        return cpuUtilizationPercent;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

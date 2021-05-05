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

package org.apache.flink.benchmark.operators;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.benchmark.operators.RecordSource.EmptyEnumeratorState;
import org.apache.flink.benchmark.operators.RecordSource.EmptySplit;
import org.apache.flink.benchmark.operators.RecordSource.Record;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A source that generates longs in a fixed number of splits. */
public class RecordSource implements Source<Record, EmptySplit, EmptyEnumeratorState> {
    public static final int PAYLOAD_SIZE = 1024;

    public static class Record {
        public long value;
        public byte[] payload;

        public Record() {
            this(0);
        }

        public Record(long value) {
            this.value = value;
            payload = new byte[PAYLOAD_SIZE];
        }
    }

    private final int minCheckpoints;

    public RecordSource(int minCheckpoints) {
        this.minCheckpoints = minCheckpoints;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Record, EmptySplit> createReader(SourceReaderContext readerContext) {
        return new RecourdSourceReader(minCheckpoints);
    }

    @Override
    public SplitEnumerator<EmptySplit, EmptyEnumeratorState> createEnumerator(
            SplitEnumeratorContext<EmptySplit> enumContext) {
        return new EmptySplitSplitEnumerator();
    }

    @Override
    public SplitEnumerator<EmptySplit, EmptyEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<EmptySplit> enumContext, EmptyEnumeratorState state) {
        return new EmptySplitSplitEnumerator();
    }

    @Override
    public SimpleVersionedSerializer<EmptySplit> getSplitSerializer() {
        return new SplitVersionedSerializer();
    }

    @Override
    public SimpleVersionedSerializer<EmptyEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new EnumeratorVersionedSerializer();
    }

    public static class RecourdSourceReader implements SourceReader<Record, EmptySplit> {
        private final int minCheckpoints;
        private int numCompletedCheckpoints;
        private long counter = 0;

        public RecourdSourceReader(int minCheckpoints) {
            this.minCheckpoints = minCheckpoints;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<Record> output) throws InterruptedException {
            output.collect(new Record(counter++));

            if (numCompletedCheckpoints >= minCheckpoints) {
                return InputStatus.END_OF_INPUT;
            }

            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<EmptySplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            numCompletedCheckpoints++;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return FutureUtils.completedVoidFuture();
        }

        @Override
        public void addSplits(List<EmptySplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() throws Exception {}
    }

    public static class EmptySplit implements SourceSplit {
        @Override
        public String splitId() {
            return "42";
        }
    }

    private static class EmptySplitSplitEnumerator
            implements SplitEnumerator<EmptySplit, EmptyEnumeratorState> {
        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

        @Override
        public void addSplitsBack(List<EmptySplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {}

        @Override
        public EmptyEnumeratorState snapshotState(long checkpointId) throws Exception {
            return new EmptyEnumeratorState();
        }

        @Override
        public void close() throws IOException {}
    }

    public static class EmptyEnumeratorState {}

    private static class EnumeratorVersionedSerializer
            implements SimpleVersionedSerializer<EmptyEnumeratorState> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(EmptyEnumeratorState state) {
            return new byte[0];
        }

        @Override
        public EmptyEnumeratorState deserialize(int version, byte[] serialized) {
            return new EmptyEnumeratorState();
        }
    }

    private static class SplitVersionedSerializer implements SimpleVersionedSerializer<EmptySplit> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(EmptySplit split) {
            return new byte[0];
        }

        @Override
        public EmptySplit deserialize(int version, byte[] serialized) {
            return new EmptySplit();
        }
    }
}

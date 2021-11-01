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

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings({"unchecked", "rawtypes"})
public class MultiplyByTwoOperatorFactory extends AbstractStreamOperatorFactory<Long> {
    @Override
    public <T extends StreamOperator<Long>> T createStreamOperator(
            StreamOperatorParameters<Long> parameters) {
        return (T) new MultiplyByTwoOperator(parameters);
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return MultiplyByTwoOperator.class;
    }

    public static class MultiplyByTwoOperator extends AbstractStreamOperatorV2<Long>
            implements MultipleInputStreamOperator<Long> {
        public MultiplyByTwoOperator(StreamOperatorParameters<Long> parameters) {
            super(parameters, 2);
        }

        @Override
        public List<Input> getInputs() {
            return Arrays.asList(
                    new MultiplyByTwoOperator.MultiplyByTwoInput(this, 1),
                    new MultiplyByTwoOperator.MultiplyByTwoInput(this, 2));
        }

        private static class MultiplyByTwoInput extends AbstractInput<Long, Long> {
            MultiplyByTwoInput(AbstractStreamOperatorV2<Long> owner, int inputId) {
                super(owner, inputId);
            }

            @Override
            public void processElement(StreamRecord<Long> element) {
                output.collect(element.replace(element.getValue() * 2));
            }
        }
    }
}

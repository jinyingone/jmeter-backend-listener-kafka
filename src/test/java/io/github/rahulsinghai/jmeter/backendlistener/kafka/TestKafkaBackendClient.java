/*
 * Copyright 2019 Rahul Singhai.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.rahulsinghai.jmeter.backendlistener.kafka;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestKafkaBackendClient {

  private static KafkaBackendClient client;
  private static BackendListenerContext context;

  @BeforeAll
  public static void setUp() throws Exception {
    client = new KafkaBackendClient();
    Arguments defaultParameters = client.getDefaultParameters();
    defaultParameters.removeArgument("kafka.bootstrap.servers");
    defaultParameters.addArgument("kafka.bootstrap.servers", "");
    defaultParameters.removeArgument("kafka.topic");
    defaultParameters.addArgument("kafka.topic", "t1");
    context = new BackendListenerContext(defaultParameters);
    client.setupTest(context);
  }

  @Test
  public void testGetDefaultParameters() {
    Arguments args = client.getDefaultParameters();
    assertNotNull(args);
  }

  @Test
  public void testhandleSampleResults() throws IOException {
    List<SampleResult> results = new LinkedList<>();
    for (int i = 0; i < 1000000; i++) {
      SampleResult e = new SampleResult();
      e.setBodySize(i);
      results.add(e);
    }
    client.handleSampleResults(Collections.singletonList(new SampleResult()), context);

    long s1 = System.currentTimeMillis();
    client.handleSampleResults(results, context);
    System.out.println(System.currentTimeMillis() - s1);
  }
}

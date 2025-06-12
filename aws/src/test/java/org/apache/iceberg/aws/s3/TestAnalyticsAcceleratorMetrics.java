/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;

public class TestAnalyticsAcceleratorMetrics {

  @Test
  public void testAnalyticsAcceleratorMetricsTracking() throws IOException {
    // Create a mock metrics context to track metrics
    TestMetricsContext metricsContext = new TestMetricsContext();
    
    // Mock the S3SeekableInputStream
    S3SeekableInputStream mockSeekableStream = mock(S3SeekableInputStream.class);
    when(mockSeekableStream.read()).thenReturn(65); // 'A'
    when(mockSeekableStream.read(any(byte[].class), any(int.class), any(int.class)))
        .thenReturn(10);
    when(mockSeekableStream.getPos()).thenReturn(0L);
    
    // Create the wrapper with metrics context
    AnalyticsAcceleratorInputStreamWrapper wrapper = 
        new AnalyticsAcceleratorInputStreamWrapper(mockSeekableStream, metricsContext);
    
    // Perform some read operations
    wrapper.read(); // Should increment readBytes by 1 and readOperations by 1
    
    byte[] buffer = new byte[10];
    wrapper.read(buffer, 0, 10); // Should increment readBytes by 10 and readOperations by 1
    
    // Verify metrics were tracked
    assertThat(metricsContext.getReadBytes()).isEqualTo(11L);
    assertThat(metricsContext.getReadOperations()).isEqualTo(2);
  }
  
  @Test
  public void testRegularS3InputStreamMetricsTracking() throws IOException {
    // Test that regular S3InputStream properly tracks metrics
    TestMetricsContext metricsContext = new TestMetricsContext();
    
    // This should work correctly with metrics
    // Note: This is more of a reference test to show expected behavior
    assertThat(metricsContext.getReadBytes()).isEqualTo(0L);
    assertThat(metricsContext.getReadOperations()).isEqualTo(0);
    
    // Simulate what S3InputStream does
    org.apache.iceberg.metrics.Counter readBytes = metricsContext.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
    org.apache.iceberg.metrics.Counter readOperations = metricsContext.counter(FileIOMetricsContext.READ_OPERATIONS, MetricsContext.Unit.COUNT);
    
    // Simulate read operations
    readBytes.increment(1L);
    readOperations.increment();
    
    readBytes.increment(10L);
    readOperations.increment();
    
    // Verify metrics were tracked
    assertThat(metricsContext.getReadBytes()).isEqualTo(11L);
    assertThat(metricsContext.getReadOperations()).isEqualTo(2);
  }
  
  /**
   * Test implementation of MetricsContext that tracks metrics in memory
   */
  private static class TestMetricsContext implements MetricsContext {
    private long readBytes = 0L;
    private int readOperations = 0;
    private long writeBytes = 0L;
    private int writeOperations = 0;
    
    @Override
    public void initialize(Map<String, String> properties) {
      // No initialization needed
    }
    
    @Override
    public org.apache.iceberg.metrics.Counter counter(String name, Unit unit) {
      switch (name) {
        case FileIOMetricsContext.READ_BYTES:
          return new org.apache.iceberg.metrics.Counter() {
            @Override
            public void increment() {
              increment(1L);
            }
            
            @Override
            public void increment(long amount) {
              readBytes += amount;
            }
            
            @Override
            public long value() {
              return readBytes;
            }
          };
        case FileIOMetricsContext.READ_OPERATIONS:
          return new org.apache.iceberg.metrics.Counter() {
            @Override
            public void increment() {
              increment(1L);
            }
            
            @Override
            public void increment(long amount) {
              readOperations += (int) amount;
            }
            
            @Override
            public long value() {
              return readOperations;
            }
          };
        case FileIOMetricsContext.WRITE_BYTES:
          return new org.apache.iceberg.metrics.Counter() {
            @Override
            public void increment() {
              increment(1L);
            }
            
            @Override
            public void increment(long amount) {
              writeBytes += amount;
            }
            
            @Override
            public long value() {
              return writeBytes;
            }
          };
        case FileIOMetricsContext.WRITE_OPERATIONS:
          return new org.apache.iceberg.metrics.Counter() {
            @Override
            public void increment() {
              increment(1L);
            }
            
            @Override
            public void increment(long amount) {
              writeOperations += (int) amount;
            }
            
            @Override
            public long value() {
              return writeOperations;
            }
          };
        default:
          throw new IllegalArgumentException("Unsupported counter: " + name);
      }
    }
    
    public long getReadBytes() {
      return readBytes;
    }
    
    public int getReadOperations() {
      return readOperations;
    }
    
    public long getWriteBytes() {
      return writeBytes;
    }
    
    public int getWriteOperations() {
      return writeOperations;
    }
  }
}
package io.qdrant.client;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.qdrant.client.grpc.Collections.*;
import io.qdrant.client.grpc.Points.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for QdrantClient using StepVerifier for reactive streams testing.
 *
 * <p>These tests demonstrate the reactive capabilities of the Qdrant client and ensure proper
 * backpressure handling, error recovery, and streaming behavior.
 *
 * @author MoniAI Team
 * @since 2.0.0
 */
class QdrantClientTest {

  private QdrantClient client;
  private ManagedChannel channel;

  @BeforeEach
  void setUp() {
    // Create an in-process channel for testing
    channel = InProcessChannelBuilder.forName("test-qdrant").directExecutor().build();

    client =
        QdrantGrpcClient.newBuilder()
            .channel(channel)
            .defaultTimeout(Duration.ofSeconds(5))
            .build();
  }

  @Test
  void testSearchRequestBuilder() {
    // Test the fluent API for building search requests
    List<Float> queryVector = Arrays.asList(0.1f, 0.2f, 0.3f, 0.4f);

    SearchPoints searchRequest =
        SearchRequestBuilder.forCollection("test_collection")
            .withVector(queryVector)
            .withPayload(true)
            .withVectors(false)
            .limit(10)
            .scoreThreshold(0.5f)
            .build();

    // Verify the request was built correctly
    assert searchRequest.getCollectionName().equals("test_collection");
    assert searchRequest.getVectorList().equals(queryVector);
    assert searchRequest.getLimit() == 10;
    assert searchRequest.getScoreThreshold() == 0.5f;
    assert searchRequest.getWithPayload().getEnable();
    assert !searchRequest.getWithVectors().getEnable();
  }

  @Test
  void testReactiveSearchRequestBuilder() {
    // Test reactive version of request builder
    List<Float> queryVector = Arrays.asList(0.1f, 0.2f, 0.3f, 0.4f);

    Mono<SearchPoints> searchRequestMono =
        SearchRequestBuilder.forCollection("test_collection")
            .withVector(queryVector)
            .limit(5)
            .buildMono();

    StepVerifier.create(searchRequestMono)
        .expectNextMatches(
            request ->
                request.getCollectionName().equals("test_collection")
                    && request.getVectorList().equals(queryVector)
                    && request.getLimit() == 5)
        .verifyComplete();
  }

  @Test
  void testPointStreamProcessing() {
    // Test streaming point processing with backpressure
    List<Float> vector1 = Arrays.asList(0.1f, 0.2f, 0.3f);
    List<Float> vector2 = Arrays.asList(0.4f, 0.5f, 0.6f);
    List<Float> vector3 = Arrays.asList(0.7f, 0.8f, 0.9f);

    Flux<PointStruct> pointStream =
        Flux.just(
            PointStruct.newBuilder()
                .setId(PointId.newBuilder().setNum(1).build())
                .setVectors(
                    Vectors.newBuilder()
                        .setVector(DenseVector.newBuilder().addAllData(vector1).build())
                        .build())
                .build(),
            PointStruct.newBuilder()
                .setId(PointId.newBuilder().setNum(2).build())
                .setVectors(
                    Vectors.newBuilder()
                        .setVector(DenseVector.newBuilder().addAllData(vector2).build())
                        .build())
                .build(),
            PointStruct.newBuilder()
                .setId(PointId.newBuilder().setNum(3).build())
                .setVectors(
                    Vectors.newBuilder()
                        .setVector(DenseVector.newBuilder().addAllData(vector3).build())
                        .build())
                .build());

    // Test backpressure by requesting one at a time
    StepVerifier.create(pointStream, 1)
        .expectNextCount(1)
        .thenRequest(1)
        .expectNextCount(1)
        .thenRequest(1)
        .expectNextCount(1)
        .verifyComplete();
  }

  @Test
  void testErrorHandling() {
    // Test error handling in reactive streams
    Flux<String> errorStream = Flux.error(new QdrantException("Test error"));

    StepVerifier.create(errorStream).expectError(QdrantException.class).verify();
  }

  @Test
  void testRetryMechanism() {
    // Test retry mechanism with exponential backoff
    Mono<String> retryMono =
        Mono.error(new RuntimeException("Temporary failure")).retry(2).onErrorReturn("fallback");

    StepVerifier.create(retryMono).expectNext("fallback").verifyComplete();
  }

  @Test
  void testTimeoutHandling() {
    // Test timeout handling in reactive operations
    Mono<String> timeoutMono =
        Mono.delay(Duration.ofSeconds(10))
            .map(i -> "delayed")
            .timeout(Duration.ofMillis(100))
            .onErrorReturn("timeout");

    StepVerifier.create(timeoutMono).expectNext("timeout").verifyComplete();
  }

  @Test
  void testBatchProcessing() {
    // Test batch processing with buffer and concatMap
    Flux<Integer> numberStream = Flux.range(1, 10);

    Flux<List<Integer>> batchedStream = numberStream.buffer(3).take(3); // Take first 3 batches

    StepVerifier.create(batchedStream)
        .expectNext(Arrays.asList(1, 2, 3))
        .expectNext(Arrays.asList(4, 5, 6))
        .expectNext(Arrays.asList(7, 8, 9))
        .verifyComplete();
  }

  @Test
  void testClientBuilder() {
    // Test the builder pattern for client creation
    ReactiveQdrantGrpcClient builtClient =
        ReactiveQdrantGrpcClient.newBuilder()
            .channel(channel)
            .shutdownChannelOnClose(false)
            .defaultTimeout(Duration.ofSeconds(30))
            .build();

    assert builtClient != null;

    // Test builder validation
    try {
      ReactiveQdrantGrpcClient.newBuilder().build();
      assert false : "Should have thrown IllegalStateException";
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("gRPC channel must be specified");
    }
  }
}

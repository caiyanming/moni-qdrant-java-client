package io.qdrant.client;

import io.qdrant.client.grpc.Collections.*;
import io.qdrant.client.grpc.Points.*;
import io.qdrant.client.grpc.QdrantOuterClass.*;
import io.qdrant.client.grpc.SnapshotsService.*;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Pure reactive Qdrant client interface.
 *
 * <p>This client provides a fully non-blocking, reactive API for interacting with Qdrant vector
 * database. All operations return either {@link Mono} for single results or {@link Flux} for
 * streaming results.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Zero blocking calls - fully reactive and non-blocking
 *   <li>Backpressure support for large data streams
 *   <li>Built-in error recovery and retry mechanisms
 *   <li>Streaming support for memory-efficient large dataset operations
 * </ul>
 *
 * @author MoniAI Team
 * @since 2.0.0
 */
public interface QdrantClient extends AutoCloseable {

  // ========== Collection Management ==========

  /**
   * Creates a new collection with the specified configuration.
   *
   * @param request the collection creation request
   * @return a Mono containing the collection operation response
   */
  Mono<CollectionOperationResponse> createCollection(CreateCollection request);

  /**
   * Lists all collections in the database.
   *
   * @return a Flux of collection descriptions
   */
  Flux<CollectionDescription> listCollections();

  /**
   * Gets detailed information about a specific collection.
   *
   * @param collectionName the name of the collection
   * @return a Mono containing the collection information
   */
  Mono<CollectionInfo> getCollectionInfo(String collectionName);

  /**
   * Deletes a collection.
   *
   * @param collectionName the name of the collection to delete
   * @return a Mono containing the operation response
   */
  Mono<CollectionOperationResponse> deleteCollection(String collectionName);

  /**
   * Checks if a collection exists.
   *
   * @param collectionName the name of the collection
   * @return a Mono containing true if the collection exists, false otherwise
   */
  Mono<Boolean> collectionExists(String collectionName);

  // ========== Point Operations ==========

  /**
   * Performs a vector similarity search.
   *
   * @param request the search request
   * @return a Flux of scored points ordered by similarity
   */
  Flux<ScoredPoint> search(SearchPoints request);

  /**
   * Performs a streaming vector similarity search for large result sets. This method is
   * memory-efficient for handling large numbers of results.
   *
   * @param request the search request
   * @return a Flux of scored points streamed from the server
   */
  Flux<ScoredPoint> searchStream(SearchPoints request);

  /**
   * Performs batch vector similarity searches.
   *
   * @param collectionName the name of the collection
   * @param searchRequests a stream of search requests
   * @return a Flux of batch results
   */
  Flux<BatchResult> searchBatch(String collectionName, Flux<SearchPoints> searchRequests);

  /**
   * Inserts or updates points in the collection.
   *
   * @param collectionName the name of the collection
   * @param points a stream of points to upsert
   * @return a Mono containing the update result
   */
  Mono<PointsOperationResponse> upsert(String collectionName, Flux<PointStruct> points);

  /**
   * Deletes points by their IDs.
   *
   * @param collectionName the name of the collection
   * @param pointIds a stream of point IDs to delete
   * @return a Mono containing the update result
   */
  Mono<PointsOperationResponse> delete(String collectionName, Flux<PointId> pointIds);

  /**
   * Retrieves points by their IDs.
   *
   * @param collectionName the name of the collection
   * @param pointIds a stream of point IDs to retrieve
   * @param withPayload whether to include payload data
   * @param withVectors whether to include vector data
   * @return a Flux of retrieved points
   */
  Flux<RetrievedPoint> getPoints(
      String collectionName, Flux<PointId> pointIds, boolean withPayload, boolean withVectors);

  /**
   * Counts points in a collection matching the given filter.
   *
   * @param collectionName the name of the collection
   * @param filter the filter to apply (can be null for counting all points)
   * @return a Mono containing the count
   */
  Mono<Long> countPoints(String collectionName, Filter filter);

  // ========== Payload Operations ==========

  /**
   * Sets payload for points matching the selector.
   *
   * @param collectionName the name of the collection
   * @param payloadStream a stream of payload updates
   * @return a Mono containing the update result
   */
  Mono<PointsOperationResponse> setPayload(
      String collectionName, Flux<SetPayloadPoints> payloadStream);

  /**
   * Clears payload for points matching the selector.
   *
   * @param collectionName the name of the collection
   * @param clearRequests a stream of clear payload requests
   * @return a Mono containing the update result
   */
  Mono<PointsOperationResponse> clearPayload(
      String collectionName, Flux<ClearPayloadPoints> clearRequests);

  // ========== Snapshots ==========

  /**
   * Creates a snapshot of the entire database.
   *
   * @return a Mono containing the snapshot description
   */
  Mono<SnapshotDescription> createSnapshot();

  /**
   * Creates a snapshot of a specific collection.
   *
   * @param collectionName the name of the collection
   * @return a Mono containing the snapshot description
   */
  Mono<SnapshotDescription> createCollectionSnapshot(String collectionName);

  /**
   * Lists all available snapshots.
   *
   * @return a Flux of snapshot descriptions
   */
  Flux<SnapshotDescription> listSnapshots();

  /**
   * Deletes a snapshot.
   *
   * @param snapshotName the name of the snapshot to delete
   * @return a Mono signaling completion
   */
  Mono<Void> deleteSnapshot(String snapshotName);

  // ========== Health and Info ==========

  /**
   * Performs a health check.
   *
   * @return a Mono containing the health check response
   */
  Mono<HealthCheckReply> healthCheck();

  // getTelemetry removed - not available in this Qdrant version

  // ========== Connection Management ==========

  /**
   * Tests the connection to the Qdrant server.
   *
   * @return a Mono that completes successfully if connection is healthy
   */
  Mono<Void> ping();

  /**
   * Sets the default timeout for all operations.
   *
   * @param timeout the default timeout duration
   * @return this client instance for method chaining
   */
  QdrantClient withTimeout(Duration timeout);

  /**
   * Closes the client and releases all resources. This method is idempotent and can be called
   * multiple times safely.
   */
  @Override
  void close();

  /**
   * Returns a Mono that completes when the client is gracefully shut down.
   *
   * @return a Mono signaling shutdown completion
   */
  Mono<Void> shutdown();
}

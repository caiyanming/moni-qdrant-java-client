package io.qdrant.client;

/**
 * Exception thrown when Qdrant operations fail.
 *
 * <p>This exception wraps underlying gRPC and reactive stream errors to provide consistent error
 * handling across the reactive client.
 *
 * @author MoniAI Team
 * @since 2.0.0
 */
public class QdrantException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new QdrantException with the specified message.
   *
   * @param message the error message
   */
  public QdrantException(String message) {
    super(message);
  }

  /**
   * Creates a new QdrantException with the specified message and cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public QdrantException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new QdrantException with the specified cause.
   *
   * @param cause the underlying cause
   */
  public QdrantException(Throwable cause) {
    super(cause);
  }
}

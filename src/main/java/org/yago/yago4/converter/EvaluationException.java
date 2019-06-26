package org.yago.yago4.converter;

public class EvaluationException extends RuntimeException {

  public EvaluationException(String message) {
    super(message);
  }

  public EvaluationException(String message, Throwable cause) {
    super(message, cause);
  }

  public EvaluationException(Throwable cause) {
    super(cause);
  }
}

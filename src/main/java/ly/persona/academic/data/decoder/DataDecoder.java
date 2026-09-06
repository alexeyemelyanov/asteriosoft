package ly.persona.academic.data.decoder;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import ly.persona.academic.data.DataReader;

public abstract class DataDecoder<R, V> implements DataReader<V> {

  private final DataReader<R> reader;
  private final Function<R, V> decodeFunction;
  private final AtomicBoolean closed;

  protected DataDecoder(DataReader<R> reader, Function<R, V> decodeFunction) {
    this.reader = Objects.requireNonNull(reader, "reader");
    this.decodeFunction = Objects.requireNonNull(decodeFunction, "decodeFunction");
    this.closed = new AtomicBoolean();
  }

  protected final R readRecord() {
    return reader.read();
  }

  protected final V decodeRecord(R record) {
    return record == null ? null : decodeFunction.apply(record);
  }

  protected final boolean isClosed() {
    return closed.get();
  }

  protected final void checkNotClosed() {
    if (isClosed()) {
      throw closedException();
    }
  }

  protected final IllegalStateException closedException() {
    return new IllegalStateException(getClass().getSimpleName() + " is already closed");
  }

  /**
   * Releases the resources of the decoder itself. Called at most once, before the underlying reader
   * is closed, so an implementation is free to finish or to abandon the reading in progress.
   */
  protected void doClose() {
  }

  /**
   * Idempotent: the underlying reader is closed exactly once and only after the decoding has been
   * stopped, so the reader is never touched after it has been closed.
   */
  @Override
  public final void close() {
    if (closed.compareAndSet(false, true)) {
      try {
        doClose();
      } finally {
        reader.close();
      }
    }
  }
}

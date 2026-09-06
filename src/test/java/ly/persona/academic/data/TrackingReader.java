package ly.persona.academic.data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A data source that remembers how it was used: how many records were requested, whether it was
 * read after being closed and how many times it was closed.
 */
public class TrackingReader implements DataReader<String> {

  private final int records;
  private final AtomicInteger reads = new AtomicInteger();
  private final AtomicInteger readsAfterClose = new AtomicInteger();
  private final AtomicInteger closeCalls = new AtomicInteger();
  private volatile boolean closed;

  public TrackingReader(int records) {
    this.records = records;
  }

  @Override
  public String read() {
    if (closed) {
      readsAfterClose.incrementAndGet();
    }
    final int index = reads.getAndIncrement();
    return index < records ? String.valueOf(index) : null;
  }

  @Override
  public void close() {
    closeCalls.incrementAndGet();
    closed = true;
  }

  public int reads() {
    return reads.get();
  }

  public int readsAfterClose() {
    return readsAfterClose.get();
  }

  public int closeCalls() {
    return closeCalls.get();
  }
}

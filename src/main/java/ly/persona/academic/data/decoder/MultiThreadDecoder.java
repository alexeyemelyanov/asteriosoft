package ly.persona.academic.data.decoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import ly.persona.academic.data.DataReader;

/**
 * Reads the source in a single background thread and decodes the records in parallel, keeping the
 * order of the source.
 * <p>
 * At most {@code maxParallelismLevel} records are read ahead of the consumer and decoded at the
 * same time, so a slow consumer slows the reading down instead of exhausting the memory.
 * <p>
 * All the threads it uses are daemon threads named after the decoder, and {@link #close()} stops
 * them. Nevertheless the decoder must be closed: otherwise the source keeps being read in the
 * background.
 */
public class MultiThreadDecoder<R, V> extends DataDecoder<R, V> {

  /** the last element of {@link #bufferOrdered}, never handed out to a caller */
  private static final Future<?> END_OF_DATA = CompletableFuture.completedFuture(null);

  private static final AtomicInteger DECODER_SEQUENCE = new AtomicInteger();

  private final String name;

  private final ExecutorService worker;

  /** the records in the order they were read, still being decoded or decoded already */
  private final BlockingQueue<Future<V>> bufferOrdered;

  /** limits how far ahead of the consumer the source may be read */
  private final Semaphore readAheadPermits;

  private final Object startLock;
  private volatile Thread backgroundReader;

  /** a failure of the background reader, reported to every caller of {@link #read()} */
  private volatile Throwable readerFailure;

  public MultiThreadDecoder(DataReader<R> reader, Function<R, V> decoder, int maxParallelismLevel) {
    super(reader, decoder);
    if (maxParallelismLevel < 1) {
      throw new IllegalArgumentException("maxParallelismLevel must be positive, but was " + maxParallelismLevel);
    }
    this.name = "data-decoder-" + DECODER_SEQUENCE.incrementAndGet();
    // one slot on top of the permits is reserved for END_OF_DATA, so publishing it never blocks
    this.bufferOrdered = new ArrayBlockingQueue<>(maxParallelismLevel + 1);
    this.readAheadPermits = new Semaphore(maxParallelismLevel);
    this.worker = Executors.newFixedThreadPool(maxParallelismLevel, daemonThreads(name + "-worker-"));
    this.startLock = new Object();
  }

  /**
   * The records are handed out in the order they were read from the source, even while concurrent
   * calls: no record is given out twice and every caller observes an increasing subsequence of the
   * source.
   *
   * @return the next decoded record, or {@code null} when the source is exhausted
   * @throws IllegalStateException if the decoder is already closed, or if the calling thread has
   *                               been interrupted while waiting for the next record
   * @throws RuntimeException      if the source or the decoding of the record has failed; the
   *                               original unchecked failure is rethrown as is
   */
  @Override
  public V read() {
    checkNotClosed();
    startBackgroundReaderIfNeeded();

    final Future<V> next = takeNext();
    if (next == END_OF_DATA || isClosed()) {
      if (next != END_OF_DATA) {
        // close() has drained the buffer and this record has slipped through just in time
        next.cancel(true);
      }
      // keep the marker in the buffer, otherwise the other callers are left waiting forever
      publishEndOfData();
      if (isClosed()) {
        throw closedException();
      }
      final Throwable failure = readerFailure;
      if (failure != null) {
        throw asUnchecked("failed to read the source", failure);
      }
      return null;
    }

    try {
      return next.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for a record to be decoded", e);
    } catch (ExecutionException e) {
      throw asUnchecked("failed to decode the record", e.getCause());
    } catch (CancellationException e) {
      // the only reason to cancel a record is the shutdown of the decoder
      throw isClosed() ? closedException() : e;
    } finally {
      readAheadPermits.release();
    }
  }

  @Override
  protected void doClose() {
    final Thread reader = backgroundReader;
    if (reader != null) {
      // the background thread parks on the source, on the permits and on the buffer: all of these
      // are interruptible, so an interrupt is the way to ask it to stop
      reader.interrupt();
      try {
        reader.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // drop the records nobody is going to read and wake up the callers waiting in read()
    final List<Future<V>> abandoned = new ArrayList<>();
    bufferOrdered.drainTo(abandoned);
    abandoned.forEach(future -> future.cancel(true));
    publishEndOfData();

    worker.shutdownNow();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + name + "]";
  }

  private void startBackgroundReaderIfNeeded() {
    if (backgroundReader != null) {
      return;
    }
    // nothing blocking may happen under the lock: close() and the other readers need it as well
    synchronized (startLock) {
      if (backgroundReader != null || isClosed()) {
        return;
      }
      final Thread reader = new Thread(this::readSourceInBackground, name + "-reader");
      reader.setDaemon(true);
      backgroundReader = reader;
      reader.start();
    }
  }

  private void readSourceInBackground() {
    try {
      while (!isClosed()) {
        readAheadPermits.acquire();
        final R record = readRecord();
        if (record == null) {
          break;
        }
        // a free slot is reserved by the acquired permit, so the buffer never blocks here
        bufferOrdered.put(worker.submit(() -> decodeRecord(record)));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt(); // this is how close() asks to stop
    } catch (Throwable e) {
      if (!isClosed()) {
        readerFailure = e;
      }
    } finally {
      publishEndOfData();
    }
  }

  /**
   * Announces that no record is going to be added to the buffer any more. Never blocks thanks to
   * the slot reserved for the marker, so a slow consumer cannot hang the shutdown, and a duplicate
   * marker left by a full buffer changes nothing: hence the ignored result.
   */
  private void publishEndOfData() {
    //noinspection ResultOfMethodCallIgnored
    bufferOrdered.offer(endOfData());
  }

  private Future<V> takeNext() {
    try {
      return bufferOrdered.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while waiting for the next record", e);
    }
  }

  @SuppressWarnings("unchecked")
  private Future<V> endOfData() {
    return (Future<V>) END_OF_DATA;
  }

  /**
   * Keeps the original failure catchable by the caller instead of hiding it behind a generic
   * wrapper.
   */
  private static RuntimeException asUnchecked(String message, Throwable cause) {
    if (cause instanceof Error error) {
      throw error;
    }
    if (cause instanceof RuntimeException runtime) {
      return runtime;
    }
    return new RuntimeException(message, cause);
  }

  private static ThreadFactory daemonThreads(String namePrefix) {
    final AtomicInteger sequence = new AtomicInteger();
    return runnable -> {
      final Thread thread = new Thread(runnable, namePrefix + sequence.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}

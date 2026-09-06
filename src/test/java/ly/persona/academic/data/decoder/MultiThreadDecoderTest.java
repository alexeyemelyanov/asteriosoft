package ly.persona.academic.data.decoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import ly.persona.academic.data.CountData;
import ly.persona.academic.data.DataGenerator;
import ly.persona.academic.data.DataReader;
import ly.persona.academic.data.SimulatedFailure;
import ly.persona.academic.data.TrackingReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * The behaviour that only the {@link MultiThreadDecoder} promises: concurrent reading, the bounded
 * read ahead and the shutdown of the background threads.
 */
public class MultiThreadDecoderTest {

  /** a bug in the synchronization must fail the build instead of hanging it */
  @Rule
  public Timeout timeout = Timeout.seconds(120);

  @Test
  public void rejectsNonPositiveParallelismLevel() {
    Assert.assertThrows(IllegalArgumentException.class,
        () -> new MultiThreadDecoder<>(new TrackingReader(1), CountData::fromCsv, 0));
    Assert.assertThrows(IllegalArgumentException.class,
        () -> new MultiThreadDecoder<>(new TrackingReader(1), CountData::fromCsv, -1));
  }

  @Test
  public void concurrentReadsShareEveryRecordExactlyOnceKeepingTheOrder() throws Exception {
    final int records = 500;
    final int readers = 8;
    final int attemptsAfterTheEnd = 50;

    try (DataDecoder<String, CountData> decoder =
             new MultiThreadDecoder<>(new DataGenerator<>(String::valueOf, records), CountData::fromCsv, 4)) {

      final List<Callable<List<Integer>>> tasks = new ArrayList<>();
      for (int reader = 0; reader < readers; reader++) {
        tasks.add(() -> {
          final List<Integer> received = new ArrayList<>();
          // null must mean the end of the data, so keep asking for a while after the first one
          int nulls = 0;
          while (nulls < attemptsAfterTheEnd) {
            final CountData data = decoder.read();
            if (data == null) {
              nulls++;
            } else if (nulls > 0) {
              throw new AssertionError("the end of the data has been reported, but record "
                  + data.count() + " has arrived after that");
            } else {
              received.add(data.count());
            }
          }
          return received;
        });
      }

      final ExecutorService readerPool = Executors.newFixedThreadPool(readers);
      final List<Integer> allReceived = new ArrayList<>();
      try {
        for (Future<List<Integer>> result : readerPool.invokeAll(tasks)) {
          final List<Integer> received = get(result);
          Assert.assertTrue("a single reader must observe an increasing subsequence of the source: "
              + received, isIncreasing(received));
          allReceived.addAll(received);
        }
      } finally {
        readerPool.shutdownNow();
      }

      Collections.sort(allReceived);
      Assert.assertEquals("every record must be read exactly once",
          IntStream.range(0, records).boxed().toList(), allReceived);
    }
  }

  @Test
  public void limitsHowFarAheadOfTheConsumerTheSourceIsRead() throws Exception {
    final int parallelism = 4;
    final TrackingReader source = new TrackingReader(Integer.MAX_VALUE);

    try (DataDecoder<String, CountData> decoder = new MultiThreadDecoder<>(source, CountData::fromCsv, parallelism)) {
      Assert.assertNotNull(decoder.read());
      // give the background reader all the time it needs to run as far ahead as it is allowed to
      Thread.sleep(500);

      Assert.assertTrue("the source must not be read further than the buffer allows, but "
          + source.reads() + " records have been requested", source.reads() <= parallelism + 1);
    }
  }

  @Test
  public void closeStopsEveryThreadItHasStarted() throws Exception {
    final Set<Thread> foreign = decoderThreads();

    final DataDecoder<String, CountData> decoder =
        new MultiThreadDecoder<>(new DataGenerator<>(String::valueOf, 100_000), CountData::fromCsv, 8);
    decoder.read();

    final Set<Thread> started = decoderThreads();
    started.removeAll(foreign);
    Assert.assertFalse("the decoder is expected to read and decode in background threads", started.isEmpty());
    for (Thread thread : started) {
      Assert.assertTrue(thread.getName() + " must be a daemon thread, otherwise it keeps the JVM alive",
          thread.isDaemon());
    }

    decoder.close();

    // the threads are interrupted, not stopped, so they need a moment to notice
    final long deadline = System.currentTimeMillis() + 30_000;
    while (started.stream().anyMatch(Thread::isAlive) && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    Assert.assertEquals("close() must not leave any thread behind",
        Collections.emptySet(),
        started.stream().filter(Thread::isAlive).map(Thread::getName).collect(Collectors.toSet()));
  }

  @Test
  public void closeUnblocksTheReaderWaitingForARecord() throws Exception {
    final CountDownLatch readingStarted = new CountDownLatch(1);
    final CountDownLatch neverReleased = new CountDownLatch(1);
    final DataReader<String> stalling = () -> {
      readingStarted.countDown();
      try {
        neverReleased.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SimulatedFailure("the reading has been interrupted");
      }
      return "0";
    };

    final DataDecoder<String, CountData> decoder = new MultiThreadDecoder<>(stalling, CountData::fromCsv, 4);
    final ExecutorService consumerPool = Executors.newSingleThreadExecutor();
    try {
      final Callable<CountData> consumer = decoder::read;
      final Future<CountData> blocked = consumerPool.submit(consumer);

      Assert.assertTrue("the source has not been touched", readingStarted.await(30, TimeUnit.SECONDS));
      Thread.sleep(100);
      Assert.assertFalse("the consumer is expected to wait for a record", blocked.isDone());

      decoder.close();

      final ExecutionException failure = Assert.assertThrows(ExecutionException.class,
          () -> blocked.get(30, TimeUnit.SECONDS));
      Assert.assertTrue("the waiting reader must be told that the decoder is closed, but got "
          + failure.getCause(), failure.getCause() instanceof IllegalStateException);
    } finally {
      consumerPool.shutdownNow();
      neverReleased.countDown();
    }
  }

  private static <T> T get(Future<T> result) throws Exception {
    try {
      return result.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AssertionError error) {
        throw error;
      }
      throw e;
    }
  }

  private static Set<Thread> decoderThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .filter(thread -> thread.getName().startsWith("data-decoder-"))
        .collect(Collectors.toCollection(HashSet::new));
  }

  private static boolean isIncreasing(List<Integer> values) {
    for (int i = 1; i < values.size(); i++) {
      if (values.get(i - 1) >= values.get(i)) {
        return false;
      }
    }
    return true;
  }
}

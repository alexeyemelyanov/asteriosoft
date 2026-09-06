package ly.persona.academic.data.decoder;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import ly.persona.academic.data.CountData;
import ly.persona.academic.data.DataGenerator;
import ly.persona.academic.data.FailingReader;
import ly.persona.academic.data.SimulatedFailure;
import ly.persona.academic.data.TrackingReader;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The contract every {@link DataDecoder} implementation has to follow.
 */
@RunWith(Parameterized.class)
public class DataDecoderTest {

  private static final int RECORDS = 2000;

  @Parameterized.Parameters(name = "{0}")
  public static Object[][] decoders() {
    return new Object[][]{
        {"SingleThreadDecoder", (DataDecoderFactory<String, CountData>) SingleThreadDecoder::new},
        {"MultiThreadDecoder(1)", (DataDecoderFactory<String, CountData>) (reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 1)},
        {"MultiThreadDecoder(100)", (DataDecoderFactory<String, CountData>) (reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 100)},
    };
  }

  @Parameterized.Parameter
  public String name;

  @Parameterized.Parameter(1)
  public DataDecoderFactory<String, CountData> factory;

  /** a bug in the synchronization must fail the build instead of hanging it */
  @Rule
  public Timeout timeout = Timeout.seconds(120);

  @Test
  public void readsEveryRecordInOrder() {
    try (DataDecoder<String, CountData> decoder = decoderOf(RECORDS)) {
      final long started = System.currentTimeMillis();
      int count = 0;
      for (CountData data = decoder.read(); data != null; data = decoder.read()) {
        Assert.assertEquals("the records are out of order", count++, data.count());
      }

      Assert.assertEquals("the reading has been cut short", RECORDS, count);
      System.out.println(name + ": " + count + " records in " + (System.currentTimeMillis() - started) + " millis");
    }
  }

  @Test
  public void readsNothingFromAnEmptySource() {
    try (DataDecoder<String, CountData> decoder = decoderOf(0)) {
      Assert.assertNull(decoder.read());
    }
  }

  @Test
  public void keepsReportingTheEndOfDataOnceItIsReached() {
    try (DataDecoder<String, CountData> decoder = decoderOf(2)) {
      Assert.assertNotNull(decoder.read());
      Assert.assertNotNull(decoder.read());
      Assert.assertNull(decoder.read());
      Assert.assertNull(decoder.read());
    }
  }

  @Test
  public void closesCleanlyAfterAPartialReading() {
    final TrackingReader source = new TrackingReader(RECORDS);
    try (DataDecoder<String, CountData> decoder = factory.createDecoder(source, CountData::fromCsv)) {
      final CountData data = decoder.read();
      Assert.assertNotNull("the first record is missing", data);
      Assert.assertEquals(0, data.count());
    }
    Assert.assertEquals("the source must be closed exactly once", 1, source.closeCalls());
  }

  @Test
  public void closesTheSourceEvenIfNothingHasBeenRead() {
    final TrackingReader source = new TrackingReader(RECORDS);
    factory.createDecoder(source, CountData::fromCsv).close();

    Assert.assertEquals("the source must be closed exactly once", 1, source.closeCalls());
    Assert.assertEquals("the source must not be read at all", 0, source.reads());
  }

  @Test
  public void closeIsIdempotent() {
    final TrackingReader source = new TrackingReader(RECORDS);
    final DataDecoder<String, CountData> decoder = factory.createDecoder(source, CountData::fromCsv);
    decoder.read();
    decoder.close();
    decoder.close();

    Assert.assertEquals("the source must be closed exactly once", 1, source.closeCalls());
  }

  @Test
  public void neverReadsTheSourceAfterItHasBeenClosed() {
    // the reading is stopped concurrently with the closing, so the race needs several attempts
    for (int attempt = 0; attempt < 50; attempt++) {
      final TrackingReader source = new TrackingReader(Integer.MAX_VALUE);
      final DataDecoder<String, CountData> decoder = factory.createDecoder(source, CountData::fromCsv);
      decoder.read();
      decoder.close();

      Assert.assertEquals("the source has been read after it was closed", 0, source.readsAfterClose());
    }
  }

  @Test
  public void readAfterCloseIsRejected() {
    final DataDecoder<String, CountData> decoder = decoderOf(RECORDS);
    decoder.read();
    decoder.close();

    Assert.assertThrows(IllegalStateException.class, decoder::read);
  }

  @Test
  public void reportsASourceFailureInsteadOfTheEndOfData() {
    final AtomicInteger read = new AtomicInteger();
    try (DataDecoder<String, CountData> decoder = factory.createDecoder(new FailingReader(50), CountData::fromCsv)) {
      final SimulatedFailure failure = Assert.assertThrows("the failure of the source has been swallowed",
          SimulatedFailure.class,
          () -> {
            for (CountData data = decoder.read(); data != null; data = decoder.read()) {
              read.incrementAndGet();
            }
          });

      Assert.assertEquals("simulated source failure at record 50", failure.getMessage());
      Assert.assertEquals("every record read before the failure must be delivered", 50, read.get());
    }
  }

  @Test
  public void reportsADecodingFailureWithItsOriginalType() {
    final Function<String, CountData> failingDecoder = record -> {
      if ("7".equals(record)) {
        throw new SimulatedFailure("cannot decode " + record);
      }
      return CountData.fromCsv(record);
    };

    try (DataDecoder<String, CountData> decoder = factory.createDecoder(new DataGenerator<>(String::valueOf, 20), failingDecoder)) {
      final SimulatedFailure failure = Assert.assertThrows(SimulatedFailure.class, () -> {
        for (int i = 0; i < 20; i++) {
          decoder.read();
        }
      });

      Assert.assertEquals("cannot decode 7", failure.getMessage());
    }
  }

  private DataDecoder<String, CountData> decoderOf(int records) {
    return factory.createDecoder(new DataGenerator<>(String::valueOf, records), CountData::fromCsv);
  }
}

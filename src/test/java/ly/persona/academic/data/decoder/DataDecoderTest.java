package ly.persona.academic.data.decoder;

import ly.persona.academic.data.CountData;
import ly.persona.academic.data.DataGenerator;
import org.junit.Assert;
import org.junit.Test;

public class DataDecoderTest {

  @Test
  public void testSingleThreadDecoder() {
    fullReading(SingleThreadDecoder::new);
    partialReading(SingleThreadDecoder::new);
    noReading(SingleThreadDecoder::new);
  }

  @Test
  public void testMultiThreadDecoder() {
    fullReading((reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 100));
    fullReading((reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 1));
    partialReading((reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 100));
    partialReading((reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 1));
    noReading((reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 100));
    noReading((reader, decoder) -> new MultiThreadDecoder<>(reader, decoder, 1));
  }

  private void fullReading(DataDecoderFactory<String, CountData> factory) {
    try (DataDecoder<String, CountData> decoder = factory.createDecoder(new DataGenerator<>(String::valueOf, 2000), CountData::fromCsv)) {
      long time = System.currentTimeMillis();
      int count = 0;
      for (CountData data = decoder.read(); data != null; data = decoder.read()) {
        // check the order
        Assert.assertEquals(count++, data.count());
      }

      time = System.currentTimeMillis() - time;
      System.out.println("Time = " + time + " millis for " + decoder.getClass().getSimpleName());
    }
  }

  private void partialReading(DataDecoderFactory<String, CountData> factory) {
    try (DataDecoder<String, CountData> decoder = factory.createDecoder(new DataGenerator<>(String::valueOf, 2000), CountData::fromCsv)) {
      CountData data = decoder.read();
      Assert.assertEquals(0, data.count());
    }
  }

  private void noReading(DataDecoderFactory<String, CountData> factory) {
    factory.createDecoder(new DataGenerator<>(String::valueOf, 2000), CountData::fromCsv).close();
  }
}

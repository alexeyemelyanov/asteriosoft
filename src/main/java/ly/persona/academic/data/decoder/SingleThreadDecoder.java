package ly.persona.academic.data.decoder;

import java.util.function.Function;
import ly.persona.academic.data.DataReader;

public class SingleThreadDecoder<R, V> extends DataDecoder<R, V> {

  public SingleThreadDecoder(DataReader<R> reader, Function<R, V> decoder) {
    super(reader, decoder);
  }

  /**
   * Reads and decodes in the calling thread. Not thread safe: concurrent calls are as safe as the
   * underlying reader is.
   *
   * @throws IllegalStateException if the decoder is already closed
   */
  @Override
  public V read() {
    checkNotClosed();
    return decodeRecord(readRecord());
  }
}

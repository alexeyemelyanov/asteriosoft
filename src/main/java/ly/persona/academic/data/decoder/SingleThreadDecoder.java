package ly.persona.academic.data.decoder;

import java.util.function.Function;
import ly.persona.academic.data.DataReader;

public class SingleThreadDecoder<R, V> extends DataDecoder<R, V> {

  public SingleThreadDecoder(DataReader<R> reader, Function<R, V> decoder) {
    super(reader, decoder);
  }

  @Override
  public V read() {
    R record = readRecord();
    return record == null ? null : decodeRecord(record);
  }
}

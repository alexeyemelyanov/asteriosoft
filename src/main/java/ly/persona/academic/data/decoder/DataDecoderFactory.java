package ly.persona.academic.data.decoder;

import java.util.function.Function;
import ly.persona.academic.data.DataReader;

public interface DataDecoderFactory<R, V> {
  DataDecoder<R, V> createDecoder(DataReader<R> reader, Function<R, V> decoder);
}

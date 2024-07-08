package ly.persona.academic.data.decoder;

import ly.persona.academic.data.DataReader;

import java.util.function.Function;

public interface DataDecoderFactory<R, V> {
    DataDecoder<R, V> createDecoder(DataReader<R> reader, Function<R, V> decoder);
}

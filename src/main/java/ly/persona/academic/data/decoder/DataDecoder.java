package ly.persona.academic.data.decoder;

import lombok.RequiredArgsConstructor;
import ly.persona.academic.data.DataReader;

import java.util.function.Function;

@RequiredArgsConstructor
public abstract class DataDecoder<R, V> implements DataReader<V> {

    private final DataReader<R> reader;
    private final Function<R, V> decodeFunction;

    protected final R readRecord() {
        return reader.read();
    }
    protected final V decodeRecord(R record) {
        return record == null ? null : decodeFunction.apply(record);
    }
    @Override
    public void close() {
        reader.close();
    }
}

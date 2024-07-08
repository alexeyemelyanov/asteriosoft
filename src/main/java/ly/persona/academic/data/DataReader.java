package ly.persona.academic.data;

import java.io.Closeable;

public interface DataReader<T> extends Closeable {
    /**
     * Null means end of the data read
     */
    T read();

    @Override
    default void close() {
    }
}

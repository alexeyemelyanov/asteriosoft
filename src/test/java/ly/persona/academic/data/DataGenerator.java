package ly.persona.academic.data;

import lombok.RequiredArgsConstructor;

import java.util.function.Function;

@RequiredArgsConstructor
public class DataGenerator<T> implements DataReader<T> {

    private final Function<Integer, T> generator;
    private final int max;
    private int count;

    public T read() {
        if (count > 0 && count % 100_000 == 0) {
            System.out.println("Generated " + count + " records");
        }
        return count >= max ? null : generator.apply(count++);
    }
}

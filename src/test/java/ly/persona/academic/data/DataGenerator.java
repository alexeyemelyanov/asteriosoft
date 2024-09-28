package ly.persona.academic.data;

import java.util.function.Function;

public class DataGenerator<T> implements DataReader<T> {

  private final Function<Integer, T> generator;
  private final int max;
  private int count;

  public DataGenerator(Function<Integer, T> generator, int max) {
    this.generator = generator;
    this.max = max;
  }

  public T read() {
    if (count > 0 && count % 100_000 == 0) {
      System.out.println("Generated " + count + " records");
    }
    return count >= max ? null : generator.apply(count++);
  }
}

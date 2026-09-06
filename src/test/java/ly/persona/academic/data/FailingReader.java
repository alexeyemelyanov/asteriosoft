package ly.persona.academic.data;

/**
 * A data source that emulates an I/O failure in the middle of the data.
 */
public class FailingReader implements DataReader<String> {

  private final int failAtRecord;
  private int index;

  public FailingReader(int failAtRecord) {
    this.failAtRecord = failAtRecord;
  }

  @Override
  public String read() {
    if (index == failAtRecord) {
      throw new SimulatedFailure("simulated source failure at record " + failAtRecord);
    }
    return String.valueOf(index++);
  }
}

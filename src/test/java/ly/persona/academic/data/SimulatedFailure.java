package ly.persona.academic.data;

/**
 * A distinct exception type, so that a test can tell a simulated failure of the data source or of
 * the decoding from a failure of the decoder itself.
 */
public class SimulatedFailure extends RuntimeException {

  public SimulatedFailure(String message) {
    super(message);
  }
}

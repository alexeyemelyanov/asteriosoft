package ly.persona.academic.data;

import lombok.Value;

@Value
public class CountData {

    int count;

    public static CountData fromCsv(String str) {
        // emulate some hard work
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // and return data
        return new CountData(Integer.parseInt(str));
    }

    public static String toCsv(CountData obj) {
        // emulate some hard work
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // and return data
        return String.valueOf(obj.getCount());
    }
}

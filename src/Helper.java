
public class Helper {
    public static int charArrayLength(char[] buffer) {
        for (int i = buffer.length - 1; i >= 0; i--) {
            if (buffer[i] != 0) {
                return i + 1;
            }
        }
        return 0;
    }
}

import java.util.List;

public class Helper {
    /**
     * Return the length of sub-array whose last char is the last non-null char of buffer
     * @param buffer raw buffer, who may have null
     * @return non-null length
     */
    public static int charArrayLength(char[] buffer) {
        for (int i = buffer.length - 1; i >= 0; i--) {
            if (buffer[i] != 0) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Because ArrayList list has a stupid property: it can not access the object which
     * exceed current size. Some elements need to be append to index
     *
     * @param list  list to be expanded
     * @param index the index you want to access
     * @param <T>   Object
     */
    @SuppressWarnings("unchecked")
    public static <T> void expandToIndex(List<T> list, int index) {
        int size = list.size();

        for (int i = 0; i < (index + 1 - size); i++) {
            list.add((T) new Object());
        }
    }
}

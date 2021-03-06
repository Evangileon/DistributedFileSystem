import java.util.List;

public class Helper {
    /**
     * Return the length of sub-array whose last char is the last non-null char of buffer
     *
     * @param buffer raw buffer, who may have null
     * @return non-null length
     */
    public static int charArrayLength(char[] buffer) {
        if (buffer == null) {
            return 0;
        }

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
     */
    public static void expandToIndexInteger(List<Integer> list, int index) {
        int size = list.size();

        for (int i = 0; i < (index + 1 - size); i++) {
            list.add(0);
        }
    }

    /**
     * Because ArrayList list has a stupid property: it can not access the object which
     * exceed current size. Some elements need to be append to index
     *
     * @param list  list to be expanded
     * @param index the index you want to access
     */
    public static void expandToIndexBoolean(List<Boolean> list, int index) {
        int size = list.size();

        for (int i = 0; i < (index + 1 - size); i++) {
            list.add(false);
        }
    }
}

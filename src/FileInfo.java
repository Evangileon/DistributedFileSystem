import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Created by Jun Yu on 11/6/14.
 */

/**
 * realFileName-UUID
 */
public class FileInfo implements Serializable {


    static final String uuidExample = "067e6162-3b6f-4ae2-a171-2470b63dff00";
    String fileDir;

    HashMap<String, ArrayList<FileChunk>> fileChunks;

    public void recoverFileInfoFromDisk() {

        fileChunks = new HashMap<>();

        final int UUIDLength = uuidExample.length();

        File folder = new File(fileDir);
        if (!folder.isDirectory()) {
            System.out.println(fileDir + " is not a directory");
            return;
        }

        File[] fileList = folder.listFiles();
        if (fileList == null) {
            return;
        }

        for (File file : fileList) {
            String fileName = file.getName();
            if (fileName.length() <= (UUIDLength + 1)) {
                continue;
            }

            int lastDash = fileName.length() - 9;
            String realName = fileName.substring(0, lastDash);
            String chunkID = fileName.substring(lastDash + 1);
            FileChunk chunk = new FileChunk(realName, Integer.valueOf(chunkID));

            ArrayList<FileChunk> oneFile = fileChunks.get(realName);
            if (oneFile == null) {
                oneFile = new ArrayList<>();
                fileChunks.put(realName, oneFile);
            }
            oneFile.add(chunk);
        }

        for (Map.Entry<String, ArrayList<FileChunk>> pair : fileChunks.entrySet()) {
            Collections.sort(pair.getValue());
        }
    }

    public static void main(String[] args) {
        String str = "000123";
        int num = Integer.valueOf(str);
        System.out.println(num);
    }
}

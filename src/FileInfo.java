import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class FileInfo implements Serializable, Iterable<Map.Entry<String, List<FileChunk>>> {

    String fileDir;

    // file chunks are in increasing order by their id
    final Map<String, List<FileChunk>> fileChunks = new ConcurrentHashMap<>();

    public FileInfo() {
    }

    public FileInfo(String dir) {
        this.fileDir = dir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;

        File dir = new File(this.fileDir);
        if (!dir.exists()) {
            dir.mkdir();
        }

        if (!dir.isDirectory()) {
            dir.delete();
            dir.mkdir();
        }
    }

    public void recoverFileInfoFromDisk() {

        File folder = new File(fileDir);
        if (!folder.isDirectory()) {
            System.out.println(fileDir + " is not a directory");
            return;
        }

        File[] fileList = folder.listFiles();
        if (fileList == null) {
            return;
        }

        // for each file entry under the directory
        for (File file : fileList) {
            try {
                if (file.isDirectory()) {
                    continue;
                }

                String fileName = file.getName();
                // ignore invalid one
                if (fileName.length() <= (8 + 1)) {
                    continue;
                }

                int lastDash = fileName.length() - 9;
                String realName = fileName.substring(0, lastDash);
                String chunkID = fileName.substring(lastDash + 1);

                // read data in file
                FileReader reader = new FileReader(this.fileDir + "/" + fileName);
                char[] buffer = new char[FileChunk.FIXED_SIZE];
                reader.read(buffer, 0, FileChunk.FIXED_SIZE);
                int actualLength = Helper.charArrayLength(buffer);

                FileChunk chunk = new FileChunk(realName, Integer.valueOf(chunkID), actualLength);
                // add chunk control block
                List<FileChunk> oneFile = fileChunks.get(realName);
                if (oneFile == null) {
                    oneFile = Collections.synchronizedList(new ArrayList<FileChunk>());
                    fileChunks.put(realName, oneFile);
                }
                oneFile.add(chunk);
            } catch (java.io.IOException e) {
                e.printStackTrace();
            }
        }

        for (Map.Entry<String, List<FileChunk>> pair : fileChunks.entrySet()) {
            // chunks are ordered by id
            Collections.sort(pair.getValue());
        }
    }

    /**
     * Get the total number of chunks maintained on this server
     * @deprecated
     * @return number of chunks
     */
    @Deprecated
    public int totalChunks() {
        int num = 0;
        for (List<FileChunk> chunks : fileChunks.values()) {
            num += chunks.size();
        }
        return num;
    }

    public void print() {
        for (Map.Entry<String, List<FileChunk>> entry : fileChunks.entrySet()) {
            System.out.print(entry.getKey() + " : ");
            for (FileChunk chunk : entry.getValue()) {
                System.out.print(chunk + " ");
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        System.out.println();
    }

    @Override
    public Iterator<Map.Entry<String, List<FileChunk>>> iterator() {
        return fileChunks.entrySet().iterator();
    }
}

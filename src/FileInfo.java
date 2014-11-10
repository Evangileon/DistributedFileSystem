import java.io.File;
import java.io.Serializable;
import java.util.*;



public class FileInfo implements Serializable, Iterable<Map.Entry<String, ArrayList<FileChunk>>> {

    String fileDir;

    HashMap<String, ArrayList<FileChunk>> fileChunks;

    public FileInfo(String dir) {
        this.fileDir = dir;
    }

    public void recoverFileInfoFromDisk() {

        fileChunks = new HashMap<>();


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
            if (fileName.length() <= (8 + 1)) {
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

    public void print() {
        for (Map.Entry<String, ArrayList<FileChunk>> entry : fileChunks.entrySet()) {
            System.out.println(entry.getKey()+" : ");
            for (FileChunk chunk : entry.getValue()) {
                System.out.print(chunk + " ");
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        String str = "000123";
        int num = Integer.valueOf(str);
        System.out.println(num);
    }

    @Override
    public Iterator<Map.Entry<String, ArrayList<FileChunk>>> iterator() {
        return fileChunks.entrySet().iterator();
    }
}

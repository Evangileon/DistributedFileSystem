import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;


public class FileInfo implements Serializable, Iterable<Map.Entry<String, ArrayList<FileChunk>>> {

    String fileDir;

    // file chunks are in increasing order by their id
    HashMap<String, ArrayList<FileChunk>> fileChunks;

    public FileInfo() {}
    public FileInfo(String dir) {
        this.fileDir = dir;
    }

    public void setFileDir(String fileDir) {
        this.fileDir = fileDir;
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

        // for each file entry under the directory
        for (File file : fileList) {
            try {
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
                ArrayList<FileChunk> oneFile = fileChunks.get(realName);
                if (oneFile == null) {
                    oneFile = new ArrayList<>();
                    fileChunks.put(realName, oneFile);
                }
                oneFile.add(chunk);
            } catch (java.io.IOException e) {
                e.printStackTrace();
            }
        }

        for (Map.Entry<String, ArrayList<FileChunk>> pair : fileChunks.entrySet()) {
            // chunks are ordered by id
            Collections.sort(pair.getValue());
        }
    }

    public void print() {
        for (Map.Entry<String, ArrayList<FileChunk>> entry : fileChunks.entrySet()) {
            System.out.print(entry.getKey() + " : ");
            for (FileChunk chunk : entry.getValue()) {
                System.out.print(chunk + " ");
            }
            System.out.println();
        }
    }

    public static void main(String[] args) {
        HashMap<String, List<Integer>> map = new HashMap<>();
        List<Integer> list = Collections.synchronizedList(new ArrayList<Integer>());
        map.put("first", list);

        list.add(123);

        List<Integer> list2 = map.get("first");

        for (Integer i : list2) {
            System.out.println(i);
        }
    }

    @Override
    public Iterator<Map.Entry<String, ArrayList<FileChunk>>> iterator() {
        return fileChunks.entrySet().iterator();
    }
}

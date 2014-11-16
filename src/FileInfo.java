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
        String str = "Proin congue sed enim nec finibus. Etiam blandit lacinia nulla, quis bibendum arcu consectetur quis. Nullam eu feugiat augue. Sed nisl lorem, porta eu eleifend et, placerat sed ligula. Praesent luctus, neque sit amet ullamcorper iaculis, ligula leo posuere sem, vitae hendrerit turpis eros vitae ligula. Donec ac varius nibh. Ut id metus quam. Praesent ullamcorper augue purus, ullamcorper malesuada ex malesuada non. Duis feugiat nulla lacinia porttitor tristique. Nunc iaculis quis velit non venenatis. Proin volutpat ullamcorper ullamcorper. Suspendisse iaculis tempus quam, elementum posuere augue ultricies sit amet. Sed pharetra gravida ipsum, non vestibulum purus suscipit nec. Integer ac facilisis ex, sed suscipit risus. Mauris feugiat ante ut viverra vestibulum. Donec nec turpis nulla. Nullam dignissim condimentum augue, at sollicitudin odio dictum nec. Phasellus scelerisque ipsum quis volutpat accumsan. Vivamus sollicitudin lectus a interdum sagittis. Praesent vel sem gravida, tristique felis id, semper diam. Quisque maximus nisl quis lectus iaculis, ut vulputate nisi varius. Donec volutpat magna sapien, nec ullamcorper lacus laoreet ut. Etiam placerat, tellus sit amet ullamcorper porta, enim diam vulputate nulla, in vehicula eros lorem nec mauris. Suspendisse sit amet dolor eget orci commodo condimentum. Donec tincidunt ultricies libero at maximus. Maecenas volutpat mi libero, condimentum sollicitudin lorem vulputate in. Nulla facilisi.Morbi eget rutrum dolor, quis venenatis diam. Proin turpis libero, dignissim sed augue nec, tempus luctus nulla. Phasellus vehicula dui nibh, sit amet egestas enim ultrices id. Donec erat quam, feugiat sed rutrum eget, rutrum ullamcorper sapien. Mauris ex libero, eleifend sed augue non, dignissim ultricies ex. In eu ultricies urna. Ut pellentesque orci non lorem porta, sed feugiat dui vehicula. Morbi pharetra nulla id fringilla molestie.Nunc rutrum a eros facilisis molestie. Cras vulputate facilisis libero, a sodales massa. Fusce malesuada orci ut libero fringilla eleifend seda. ";
        System.out.println(str.length());
    }

    @Override
    public Iterator<Map.Entry<String, ArrayList<FileChunk>>> iterator() {
        return fileChunks.entrySet().iterator();
    }
}

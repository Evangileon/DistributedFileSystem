/**
 * Created by evangileon on 11/6/14.
 */
public class FileChunk implements Comparable<FileChunk> {
    public static final int size = 8192;
    String realFileName;
    int chunckID;


    public FileChunk(String realFileName, int chunckID) {
        this.realFileName = realFileName;
        this.chunckID = chunckID;
    }

    public String getChunkName() {
        return realFileName + "." + chunckID;
    }

    @Override
    public int compareTo(FileChunk o) {
        return this.chunckID - o.chunckID;
    }
}

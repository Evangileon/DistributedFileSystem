import java.util.*;

public class LoadBalancer {

    final List<FileServer> loadList = Collections.synchronizedList(new ArrayList<FileServer>());

    public LoadBalancer(Collection<FileServer> loadList) {
        this.loadList.addAll(loadList);
    }

    /**
     * Sort by the order of number of chunks stored
     */
    private void sort() {
        synchronized (loadList) {
            Collections.sort(loadList, new Comparator<FileServer>() {
                @Override
                public int compare(FileServer o1, FileServer o2) {
                    return o1.fileInfo.totalChunks() - o2.fileInfo.totalChunks();
                }
            });
        }
    }

    /**
     * Get the id of file server that has least chunks maintain
     *
     * @return id of file server
     */
    public int getTop() {
        sort();
        if (loadList.size() != 0) {
            return loadList.get(0).id;
        } else {
            return 0;
        }
    }

    /**
     * File server join to the group of load balancer
     */
    public void join(FileServer fileServer) {
        synchronized (loadList) {
            loadList.add(fileServer);
        }

        sort();
    }

    /**
     * File server leave the group of load balancer
     *
     * @return true if load list contained the file server
     */
    public boolean leave(FileServer fileServer) {
        boolean left;
        synchronized (loadList) {
            left = loadList.remove(fileServer);
        }
        return left;
    }
}

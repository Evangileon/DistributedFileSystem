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
     * @param id file server that is primary
     * @return list of id of replica file server
     */
    public List<Integer> getReplicas(int id) {
        if (loadList.size() < 3) {
            return null;
        }

        sort();

        ArrayList<Integer> replicas = new ArrayList<>();
        synchronized (loadList) {
            Iterator<FileServer> itor = loadList.iterator();
            while (replicas.size() != 2 && itor.hasNext()) {
                FileServer candidate = itor.next();
                if (candidate.id != id) {
                    // good replica
                    replicas.add(candidate.id);
                }
            }
        }

        if (replicas.size() != 2) {
            return null;
        }
        return replicas;
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

import java.util.*;

public class LoadBalancer {

    //final List<FileServer> loadList = Collections.synchronizedList(new ArrayList<FileServer>());

    MetaServer metaServer;

    public LoadBalancer(MetaServer metaServer) {
        this.metaServer = metaServer;
    }

    /**
     * Sort by the order of number of chunks stored
     */
    private List<Integer> sort() {

        final Map<Integer, Integer> map = metaServer.getChunkNumberMap();
        ArrayList<Integer> loadList = new ArrayList<>();
        for (Integer id : map.keySet()) {
            loadList.add(id);
        }

        Collections.sort(loadList, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return map.get(o1) - map.get(o2);
            }
        });

        return loadList;
    }

    /**
     * Get the id of file server that has least chunks maintain
     *
     * @param id file server that is primary
     * @return list of id of replica file server
     */
    public List<Integer> getReplicas(int id) {

        List<Integer> loadList = sort();

        ArrayList<Integer> replicas = new ArrayList<>();
        Iterator<Integer> itor = loadList.iterator();
        while (replicas.size() != 2 && itor.hasNext()) {
            Integer candidate = itor.next();
            if (candidate != id) {
                // good replica
                replicas.add(candidate);
            }
        }

        if (replicas.size() != 2) {
            return null;
        }
        return replicas;
    }

    /**
     * @param previousReplicas replicas before fail
     * @return proper file server
     */
    public int getExclusiveReplica(List<Integer> previousReplicas) {
        List<Integer> loadList = sort();

        Iterator<Integer> itor = loadList.iterator();
        while (itor.hasNext()) {
            Integer candidate = itor.next();
            if (!previousReplicas.contains(candidate)) {
                // good replica
                return candidate;
            }
        }

        return 1;
    }
}

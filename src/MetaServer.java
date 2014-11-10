/**
 * @author Jun Yu on 11/5/14.
 */

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.*;
import java.util.*;

public class MetaServer {

    String hostname;

    InetAddress metaServerAddress;
    int receiveHeartbeatPort;
    int clientPort;

    ServerSocket receiveHeartbeatSock;
    ServerSocket receiveRequestSock;

    // String -> list of Chunks -> location
    final HashMap<String, List<Integer>> fileChunkMap = new HashMap<>();
    final HashMap<String, List<Boolean>> fileChunkAvailableMap = new HashMap<>();

    // file server id -> file info
    final HashMap<Integer, FileInfo> fileServerInfoMap = new HashMap<>();

    // pending file chunks not send to file servers
    // file name -> hash map to chunk id -> file server id expected to store
    final HashMap<String, HashMap<Integer, Integer>> pendingFileChunks = new HashMap<>();

    // fail times of heartbeat correspondent to id
    final HashMap<Integer, Integer> fileServerFailTimes = new HashMap<>();

    int timeoutMillis;

    HashMap<Integer, FileServer> allFileServerList;

    public MetaServer() {
    }


    public MetaServer(Node serverNode) {
        parseXMLToConfigMetaServer(serverNode);
    }

    public void resolveAddress() {
        if (hostname == null) {
            return;
        }

        try {
            metaServerAddress = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Parse XML to acquire hostname, ports of process
     *
     * @param filename xml
     */
    private void parseXML(String filename) {
        allFileServerList = new HashMap<>();

        File fXmlFile = new File(filename);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;

        try {
            dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
            //Node root = doc.getDocumentElement();
            if (!doc.hasChildNodes()) {
                System.exit(0);
            }
            doc.normalize();

            // config for meta server
            Node metaServerNode = doc.getElementsByTagName("metaServer").item(0);
            parseXMLToConfigMetaServer(metaServerNode);
            // config for file server virtual machine
            parseXMLToConfigFileServers(doc);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    private void parseXMLToConfigFileServers(Document doc) {
        Node fileServers = doc.getElementsByTagName("fileServers").item(0);
        NodeList fileServerList = fileServers.getChildNodes();

        for (int i = 0; i < fileServerList.getLength(); i++) {
            Node oneServer = fileServerList.item(i);
            if (oneServer.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            NodeList serverConfig = oneServer.getChildNodes();
            int id = 0;


            for (int j = 0; j < serverConfig.getLength(); j++) {
                Node oneConfig = serverConfig.item(j);
                if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }

                if (oneConfig.getNodeName().equals("id")) {
                    id = Integer.parseInt(oneConfig.getTextContent());
                }

            }
            this.allFileServerList.put(id, new FileServer(id, oneServer));
        }
    }

    private void parseXMLToConfigMetaServer(Node serverNode) {
        NodeList serverConfig = serverNode.getChildNodes();

        for (int j = 0; j < serverConfig.getLength(); j++) {
            Node oneConfig = serverConfig.item(j);
            if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            if (oneConfig.getNodeName().equals("hostname")) {
                this.hostname = oneConfig.getTextContent();
            }
            if (oneConfig.getNodeName().equals("receiveHeartbeatPort")) {
                this.receiveHeartbeatPort = Integer.parseInt(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("clientPort")) {
                this.clientPort = Integer.parseInt(oneConfig.getTextContent());
            }
        }
    }

    /**
     * Wait for heartbeat connection
     */
    public void prepareToReceiveHeartbeat() {
        System.out.println("Meta server receive heartbeat port: " + receiveHeartbeatPort);
        try {
            receiveHeartbeatSock = new ServerSocket(receiveHeartbeatPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                Socket fileServerSock = receiveHeartbeatSock.accept();
                int id = identifyHeartbeatConnection(fileServerSock);
                if (id < 0) {
                    System.out.println("Unknown address; " + fileServerSock.getInetAddress());
                    continue;
                }

                System.out.println("Heartbeat connection from: " + fileServerSock.getInetAddress().toString());
                // one file server, one heartbeat thread
                HeartbeatEntity oneHeartbeatEntity = new HeartbeatEntity(id, fileServerSock);
                Thread thread = new Thread(oneHeartbeatEntity);
                thread.setDaemon(true);
                thread.start();

            } catch (IOException e) {
                if (e instanceof SocketException) {
                    System.out.println(e.toString());
                    break;
                }

                e.printStackTrace();
            }
        }
    }

    public void prepareToReceiveClientRequest() {
        try {
            receiveRequestSock = new ServerSocket(clientPort);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        Thread requestHandleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket clientSock = receiveRequestSock.accept();

                        System.out.println("Receive request from: " + clientSock.getInetAddress().toString());

                        ResponseFileRequestEntity responseFileRequestEntity = new ResponseFileRequestEntity(clientSock);
                        Thread threadResponse = new Thread(responseFileRequestEntity);
                        threadResponse.setDaemon(true);
                        threadResponse.start();
                    } catch (IOException e) {

                        if (e instanceof SocketException) {
                            break;
                        }

                        e.printStackTrace();
                    }
                }
                System.out.println("Exit request handle loop");
            }
        });

        requestHandleThread.setDaemon(true);
        requestHandleThread.start();
    }

    public int identifyHeartbeatConnection(Socket fileServerSock) {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            if (pair.getValue().fileServerAddress.equals(fileServerSock.getInetAddress())) {
                return pair.getKey();
            }
        }

        return -1;
    }

    /**
     * Notify meta server that file server with id fails
     *
     * @param id of file server
     */
    public void fileServerFail(int id) {

        System.out.println("File server fail: " + id);

        // update availability
        synchronized (fileChunkAvailableMap) {
            FileInfo fileInfo = fileServerInfoMap.get(id);

            for (Map.Entry<String, ArrayList<FileChunk>> pair : fileInfo) {
                String fileName = pair.getKey();
                ArrayList<FileChunk> fileChunks = pair.getValue();

                List<Boolean> availableMap;
                availableMap = fileChunkAvailableMap.get(fileName);
                if (availableMap == null) {
                    availableMap = new ArrayList<>();
                    fileChunkAvailableMap.put(fileName, availableMap);
                }

                for (FileChunk fileChunk : fileChunks) {
                    expandToIndex(availableMap, fileChunk.chunkID);
                    availableMap.set(fileChunk.chunkID, false);
                }
            }
        }

    }

    private void resolveAllFileServerAddress() {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            pair.getValue().resolveAddress();
        }
    }

    public void launch() {
        resolveAllFileServerAddress();

        prepareToReceiveClientRequest();

        prepareToReceiveHeartbeat();
    }

    public void synchronizeWithMap(int id, FileInfo fileInfo) {

        // for each record of file chunks on file server
        for (Map.Entry<String, ArrayList<FileChunk>> pair : fileInfo) {
            String fileName = pair.getKey();
            ArrayList<FileChunk> fileChunks = pair.getValue();

            List<Integer> chunksOnThisServer;
            // must be mutually exclusive
            synchronized (fileChunkMap) {
                chunksOnThisServer = fileChunkMap.get(fileName);
                if (chunksOnThisServer == null) {
                    chunksOnThisServer = Collections.synchronizedList(new ArrayList<Integer>());
                    fileChunkMap.put(fileName, (chunksOnThisServer));
                }
            }

            // synchronize
            synchronized (chunksOnThisServer) {
                for (FileChunk fileChunk : fileChunks) {
                    expandToIndex(chunksOnThisServer, fileChunk.chunkID);
                    chunksOnThisServer.set(fileChunk.chunkID, id);
                }
            }

            // update availability
            List<Boolean> availableMap;
            synchronized (fileChunkAvailableMap) {
                availableMap = fileChunkAvailableMap.get(fileName);
                if (availableMap == null) {
                    availableMap = Collections.synchronizedList(new ArrayList<Boolean>());
                    fileChunkAvailableMap.put(fileName, availableMap);
                }
            }

            synchronized (availableMap) {
                for (FileChunk fileChunk : fileChunks) {
                    expandToIndex(availableMap, fileChunk.chunkID);
                    availableMap.set(fileChunk.chunkID, true);
                }
            }
        }

        synchronized (fileServerInfoMap) {
            fileServerInfoMap.put(id, fileInfo);
        }

    }

    /**
     * Release the pending file chunks, that means
     *
     * @param id       the id from which the file info sent
     * @param fileInfo transited from file servers
     */
    public void releasePendingChunks(int id, FileInfo fileInfo) {
        for (Map.Entry<String, ArrayList<FileChunk>> fileChunksInFileServer : fileInfo) {
            String fileName = fileChunksInFileServer.getKey();
            if (!pendingFileChunks.containsKey(fileName)) {
                continue;
            }

            HashMap<Integer, Integer> pendingChunks = pendingFileChunks.get(fileName);
            ArrayList<FileChunk> chunks = fileChunksInFileServer.getValue();
            for (FileChunk chunk : chunks) {
                int chunkID = chunk.chunkID;

                // check whether ID equal to keep consistent
                if (pendingChunks.containsKey(chunkID) && pendingChunks.get(chunkID) == id) {
                    pendingChunks.remove(chunkID);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> void expandToIndex(List<T> list, int index) {
        int size = list.size();

        for (int i = 0; i < (index + 1 - size); i++) {
            list.add((T) new Object());
        }
    }

    class HeartbeatEntity implements Runnable {
        Socket fileServerSock;

        int id;

        public HeartbeatEntity(int id, Socket fileServerSock) {
            this.id = id;
            this.fileServerSock = fileServerSock;
            fileServerSock.getInetAddress();

            fileServerFailTimes.put(id, 0);


            try {
                fileServerSock.setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {

            System.out.println("Enter meta server heartbeat receive loop");
            while (true) {
                try {
                    InputStream tcpFlow = fileServerSock.getInputStream();
                    ObjectInputStream objectInput = new ObjectInputStream(tcpFlow);

                    FileInfo fileInfo = (FileInfo) objectInput.readObject();

                    fileInfo.print();
                    //System.out.println("fileInfo printed");

                    synchronizeWithMap(this.id, fileInfo);
                    releasePendingChunks(this.id, fileInfo);

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();


                    break;
                }
            }
            System.out.println("Exit meta server heartbeat receive loop");

            try {
                fileServerSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("id " + id + " heartbeat exit");
        }
    }

    class ResponseFileRequestEntity implements Runnable {

        Socket clientSock;

        public ResponseFileRequestEntity(Socket clientSock) {
            this.clientSock = clientSock;
        }

        @Override
        public void run() {

            try {
                Socket clientSock = receiveRequestSock.accept();

                ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());
                RequestEnvelop request = (RequestEnvelop) input.readObject();

                String command = request.cmd;
                String fileName = request.fileName;

                System.out.println(command + "|" + fileName);
                ResponseEnvelop response = new ResponseEnvelop(request);
                System.out.println("Response UUID: " + response.uuid.toString());

                ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());
                output.writeObject(response);

                if (command.length() != 1) {
                    return;
                }

                char cmd = command.charAt(0);

                switch (cmd) {
                    case 'r':
                        // TODO read file
                        break;
                    case 'a':
                        // TODO append file
                        break;
                    case 'w':
                        // TODO write file
                        break;
                    case 'd':
                        // TODO delete file
                        break;
                    default:
                        System.out.println("Unknown command: " + cmd);
                }

                clientSock.close();

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

            System.out.println("Exit response to: " + clientSock.getInetAddress().toString());
        }
    }

    public static void main(String[] args) {
        MetaServer metaServer = new MetaServer();
        metaServer.parseXML("./configs.xml");
        metaServer.launch();

    }
}

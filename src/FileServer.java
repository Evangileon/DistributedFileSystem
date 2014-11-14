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
import javax.xml.xpath.*;
import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;


public class FileServer {

    int id;
    String hostname;

    InetAddress fileServerAddress;
    int receiveMetaFilePort; // receive from meta server
    int requestFilePort; // request from client
    int fileServerExchangePort; // the port this server opens to other file servers

    MetaServer metaServer;

    final FileInfo fileInfo = new FileInfo();
    String storageDir;

    // socket to send heartbeat
    Socket heartbeatSock;
    // socket to listen to request
    ServerSocket requestSock;

    HashMap<Integer, FileServer> allFileServerList;

    //public static final int CHUNK_LENGTH = 8192;

    public FileServer() {

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
            metaServer = new MetaServer(metaServerNode);
            // config for this file server
            String hostName = InetAddress.getLocalHost().getHostName();
            XPathFactory xPathFactory = XPathFactory.newInstance();
            XPath xPath = xPathFactory.newXPath();
            Node thisFileServerNode = getFileServerNodeWithHostname(doc, xPath, hostName);
            // TODO XPath to find proper file server config for this
            //Node thisFileServerNode = doc.getElementsByTagName("fileServer").item(0);
            parseXMLToConfigFileServer(thisFileServerNode);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    public static Node getFileServerNodeWithHostname(Document doc, XPath xPath, String hostname) {
        NodeList nodes = null;
        try {
            XPathExpression expr = xPath.compile(String.format("/configs/fileServers/fileServer[hostname='%s']", hostname));
            nodes = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);


        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

        if (nodes == null || nodes.getLength() == 0) {
            return null;
        }

        return nodes.item(0);
    }

    public FileServer(int id, Node serverNode) {
        this.id = id;
        parseXMLToConfigFileServer(serverNode);
    }

    public void resolveAddress() {
        if (hostname == null) {
            return;
        }

        try {
            fileServerAddress = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private void parseXMLToConfigFileServer(Node serverNode) {

        NodeList serverConfig = serverNode.getChildNodes();

        for (int j = 0; j < serverConfig.getLength(); j++) {
            Node oneConfig = serverConfig.item(j);
            if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            if (oneConfig.getNodeName().equals("id")) {
                this.id = Integer.valueOf(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("hostname")) {
                this.hostname = oneConfig.getTextContent();
            }
            if (oneConfig.getNodeName().equals("receiveMetaFilePort")) {
                this.receiveMetaFilePort = Integer.parseInt(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("requestFilePort")) {
                this.requestFilePort = Integer.parseInt(oneConfig.getTextContent());
            }
            if (oneConfig.getNodeName().equals("storageDir")) {
                this.storageDir = oneConfig.getTextContent();
            }
        }
    }

    /**
     * Create a new thread to send file meta data along with heartbeat message
     */
    private void heartbeatToMetaServer() {
        System.out.println("Meta server heartbeat: " + metaServer.hostname);
        System.out.println("Meta server heartbeat: " + metaServer.metaServerAddress.toString() + ":" + metaServer.receiveHeartbeatPort);

        while (true) {
            try {
                heartbeatSock = new Socket(metaServer.metaServerAddress, metaServer.receiveHeartbeatPort);

                //##############################################################
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Enter heartbeat send loop");
                        ObjectOutputStream output;
                        while (true) {
                            try {
                                output = new ObjectOutputStream(heartbeatSock.getOutputStream());
                                output.writeObject(fileInfo);
                                output.flush();
                                System.out.println("fileInfo flushed");
                                //output.close();
                            } catch (IOException e) {
                                System.out.println(heartbeatSock.getRemoteSocketAddress().toString());
                                e.printStackTrace();
                                break;
                            }
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.println("Exit heartbeat send loop");
                    }
                });
                thread.setDaemon(true);
                thread.start();
                //##############################################################

                try {
                    thread.join();
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }

                System.out.println("heartbeat reconnect");

            } catch (IOException e) {
                //System.out.println(heartbeatSock.getInetAddress().toString());
                e.printStackTrace();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    /**
     * Create a thread to listen to request from clients or meta server
     */
    private void prepareToReceiveRequest() {
        try {
            requestSock = new ServerSocket(requestFilePort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        final Thread requestHandleRequest = new Thread(new Runnable() {
            @Override
            public void run() {
                // note that meta server can also be requester
                while (true) {
                    try {
                        Socket clientSock = requestSock.accept();
                        System.out.println("Receive request from: " + clientSock.getInetAddress().toString());

                        ResponseRequestEntity responseRequestEntity = new ResponseRequestEntity(clientSock);
                        Thread threadResponse = new Thread(responseRequestEntity);
                        threadResponse.setDaemon(true);
                        threadResponse.run();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        requestHandleRequest.setDaemon(true);
        requestHandleRequest.start();
    }

    /**
     * Thread to handle request
     */
    class ResponseRequestEntity implements Runnable {

        Socket clientSock;

        public ResponseRequestEntity(Socket clientSock) {
            this.clientSock = clientSock;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());
                    RequestEnvelop request = (RequestEnvelop) input.readObject();

                    ResponseEnvelop response = new ResponseEnvelop(request);

                    String cmd = request.cmd;
                    String fileName = request.fileName;
                    int chunkID;
                    int offset;
                    int length;
                    int ret;

                    switch (cmd.charAt(0)) {
                        case 'r':
                            chunkID = Integer.valueOf(request.params.get(0));
                            offset = Integer.valueOf(request.params.get(1));
                            length = Integer.valueOf(request.params.get(2));
                            FileChunk chunk = getChunk(fileName, chunkID);
                            char[] data = readChunk(chunk);
                            response.setData(Arrays.copyOfRange(data, offset, offset + length));
                            break;
                        case 'w':
                            chunkID = Integer.valueOf(request.params.get(0));
                            int actualLength = Helper.charArrayLength(request.data);
                            FileChunk chunk1 = new FileChunk(fileName, chunkID, actualLength);
                            writeChunk(chunk1, request.data);
                            addToMetaData(chunk1);
                            break;
                        case 'a':
                            chunkID = Integer.valueOf(request.params.get(0));
                            FileChunk chunk2 = getChunk(fileName, chunkID);
                            FileChunk oldChunk = getChunk(fileName, chunkID);
                            ret = appendChunk(chunk2, request.data);
                            if (ret < 0 || ret != request.data.length) {
                                response.setError(FileClient.FILE_LENGTH_EXCEED);
                                break;
                            }
                            chunk2.acutualLength = oldChunk.acutualLength + ret;
                            updateMetaData(chunk2);
                            break;
                        default:
                            System.out.println("Unknown command");
                            response.setError(FileClient.INVALID_COMMAND);
                    }

                    ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());
                    output.writeObject(response);
                    output.flush();

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Get file chunk by file name and ID
     * @param fileName real file
     * @param chunkID ID
     * @return chunk if found, otherwise null
     */
    private FileChunk getChunk(String fileName, int chunkID) {
        ArrayList<FileChunk> chunkMap = fileInfo.fileChunks.get(fileName);
        if (chunkMap == null) {
            return null;
        }
        for (FileChunk chunk : chunkMap) {
            if (chunk.chunkID == chunkID) {
                return chunk;
            }
        }
        return null;
    }

    /**
     * Read file chunk from disk
     *
     * @param chunk chunk controller block
     * @return the data read
     */
    private char[] readChunk(FileChunk chunk) {
        if (chunk == null) {
            return null;
        }

        String filePath = storageDir + "/" + chunk.getChunkName();

        try {
            FileReader reader = new FileReader(filePath);

            char[] buffer = new char[FileChunk.FIXED_SIZE];
            int size = reader.read(buffer, 0, FileChunk.FIXED_SIZE);

            if (size != FileChunk.FIXED_SIZE) {
                System.out.println("Chunk size not equals to " + FileChunk.FIXED_SIZE);
                return null;
            }

            return buffer;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Write data in buffer to path specified by file chunk block
     *
     * @param chunk  controller block
     * @param buffer buffer to written
     * @return -1 if write fails, otherwise the actual size written
     */
    private int writeChunk(FileChunk chunk, char[] buffer) {
        if (chunk == null) {
            return -1;
        }

        String filePath = storageDir + "/" + chunk.getChunkName();

        try {
            File file = new File(filePath);
            boolean bMk = file.mkdirs();
            boolean bCr = file.createNewFile();

            if (!bMk) {
                System.out.println();
            }
            if (!bCr) {
                System.out.println("File " + filePath + " not created");
            }

            FileWriter writer = new FileWriter(file);

            if (buffer == null) {
                // create empty file
                return 0;
            }

            writer.write(buffer, 0, buffer.length);
            return buffer.length;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Add chunk information to meta data
     * @param chunk control block
     */
    private void addToMetaData(FileChunk chunk) {
        if (chunk == null) {
            return;
        }

        synchronized (fileInfo) {
            ArrayList<FileChunk> chunkMap = fileInfo.fileChunks.get(chunk.realFileName);
            if (chunkMap == null) {
                chunkMap = new ArrayList<>();
                fileInfo.fileChunks.put(chunk.realFileName, chunkMap);
            }
            chunkMap.add(chunk);
            Collections.sort(chunkMap);
        }
    }

    /**
     * Update the meta data, mainly update actual length
     * @param chunk control block
     */
    public void updateMetaData(FileChunk chunk) {
        if (chunk == null) {
            return;
        }

        ArrayList<FileChunk> list = fileInfo.fileChunks.get(chunk.realFileName);
        if (list == null) {
            return;
        }
        for (FileChunk ck : list) {
            if (ck.chunkID == chunk.chunkID) {
                ck.acutualLength = chunk.acutualLength;
                return;
            }
        }
    }

    /**
     * Append data at the end of specified chunk, the size of data can not exceed
     * the remaining space of chunk. (null filled space). If exceed, buffer got truncated
     *
     * @param chunk  to be appended
     * @param buffer data
     * @return -1 if fails, otherwise the actual size of data appended
     */
    public int appendChunk(FileChunk chunk, char[] buffer) {
        if (chunk == null) {
            return -1;
        }

        // get previous data
        char[] data = readChunk(chunk);
        if (data == null) {
            System.out.println("Append must occur when the specified chunk already in disk");
            return -1;
        }
        int dataLength = Helper.charArrayLength(data);

        // fill the data with content in buffer
        int actualLengthAppended = Math.min(FileChunk.FIXED_SIZE - dataLength, buffer.length);
        System.arraycopy(buffer, 0, data, dataLength, actualLengthAppended);

        // overwrite old chunk
        writeChunk(chunk, data);

        return actualLengthAppended;
    }

    /**
     * Run this before any procedure
     */
    private void initialize() {
        fileInfo.setFileDir(this.storageDir);
        fileInfo.recoverFileInfoFromDisk();
        metaServer.resolveAddress();
        resolveAddress();
    }

    /**
     * Launch all processes
     */
    public void launch() {
        initialize();

        heartbeatToMetaServer();

        prepareToReceiveRequest();
    }

    public static void main(String[] args) {
        FileServer fileServer = new FileServer();
        fileServer.parseXML("configs.xml");
        fileServer.launch();
    }
}

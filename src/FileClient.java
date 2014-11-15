import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.Socket;
import java.util.*;


public class FileClient {
    
    public static final int SUCCESS = 0;
    public static final int CHUNK_NOT_AVAILABLE = -1;
    public static final int CHUNK_IN_PENDING = -2;
    public static final int FILE_SERVER_NOT_AVAILABLE = -3;
    public static final int META_SERVER_NOT_AVAILABLE = -4;
    public static final int CAUSAL_ORDERING_VIOLATED = -5;
    public static final int FILE_NOT_EXIST = -6;
    public static final int FILE_LENGTH_EXCEED = -7;
    public static final int INVALID_COMMAND = -8;

    HashMap<Integer, FileServer> allFileServerList;

    String metaHostName; // hostname of meta server
    int metaClientPort; // port of meta server to receive client request

    //Socket clientSock;

    public FileClient(String xmlFile) {
        allFileServerList = new HashMap<>();
        parseXML(xmlFile);
    }

    public int execute(String[] params) {
        Socket clientSock;
        try {
            System.out.println("Client connect to meta server: " + metaHostName + ":" + metaClientPort);
            clientSock = new Socket(metaHostName, metaClientPort);
        } catch (IOException e) {
            e.printStackTrace();
            return META_SERVER_NOT_AVAILABLE;
        }

        RequestEnvelop request = new RequestEnvelop(params[0], params[1]);

        switch (request.cmd.charAt(0)) {
            case 'r':
                request.addParam(params[2]); // offset
                request.addParam(params[3]); // length
                break;
            case 'w':
                request.addParam(Integer.toString(params[2].length())); // length
                break;
            case 'a':

                break;
            default:
        }

        int error = sendRequestToMeta(request, clientSock);
        if (error < 0) {
            System.out.println("Meta server is not available");
            return META_SERVER_NOT_AVAILABLE;
        }

        ResponseEnvelop response = receiveResponseFromMeta(clientSock);
        if (response == null) {
            System.out.println("Meta server is not available");
            return META_SERVER_NOT_AVAILABLE;
        }

        if (!request.uuid.equals(response.requestCopy.uuid)) {
            System.out.println("UUID of request and response not equal");
            return CAUSAL_ORDERING_VIOLATED;
        }

        //System.out.println("Response UUID: " + response.uuid.toString());
        int chunkID;
        int offset;
        int length;
        String fileName;

        switch (response.requestCopy.cmd.charAt(0)) {
            case 'r':
                fileName = response.requestCopy.fileName;
                offset = Integer.valueOf(response.requestCopy.params.get(0));
                length = Integer.valueOf(response.requestCopy.params.get(1));

                String data = readData(fileName, offset, length, response.chunksToScan, response.chunksLocation);
                if (data != null) {
                    System.out.println(data);
                }
                break;
            case 'w':
                fileName = response.requestCopy.fileName;
                int ret = writeData(params[params.length - 1], fileName, response.chunksToScan, response.chunksLocation);
                if (ret >=0) {
                    System.out.printf("Write success");
                }

                break;
            case 'a':

                break;
            default:
        }

        try {
            clientSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Read all data from chunk
     * @param fileServerID file server ID
     * @param fileName file name demanded
     * @param chunkID chunk ID for the file
     * @return data if success, otherwise null
     */
    private char[] readChunkData(int fileServerID, String fileName, int chunkID) {
        return readChunkData(fileServerID, fileName, chunkID, 0, FileChunk.FIXED_SIZE);
    }

    /**
     * Read data from chunk
     * @param fileServerID file server ID
     * @param fileName file name demanded
     * @param chunkID chunk ID for the file
     * @param offset offset inside the chunk
     * @param length data length
     * @return data if success, otherwise null
     */
    private char[] readChunkData(int fileServerID, String fileName, int chunkID, int offset, int length) {
        FileServer fileServer = allFileServerList.get(fileServerID);
        try {
            Socket fileSock = new Socket(allFileServerList.get(fileServerID).hostname, fileServer.requestFilePort);
            RequestEnvelop request = new RequestEnvelop("r", fileName);
            request.addParam(Integer.toString(chunkID));
            request.addParam(Integer.toString(offset));
            request.addParam(Integer.toString(length));

            ObjectOutputStream output = new ObjectOutputStream(fileSock.getOutputStream());
            output.writeObject(request);
            output.flush();

            ObjectInputStream input = new ObjectInputStream(fileSock.getInputStream());
            ResponseEnvelop response = (ResponseEnvelop) input.readObject();

            input.close();

            fileSock.close();
            return response.data;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Read data from file servers according information in returned response
     * @param fileName demanded file
     * @param offset offset in first chunk of returned
     * @param length data length
     * @param chunksToScan chunk list need to retrieve
     * @param chunksLocation file server ID
     * @return concatenated data
     */
    public String readData(String fileName, int offset, int length,  List<Integer> chunksToScan, List<Integer> chunksLocation) {
        StringBuilder result = new StringBuilder();

        offset = offset % FileChunk.FIXED_SIZE;

        Iterator<Integer> chunkItor = chunksToScan.iterator();
        Iterator<Integer> locationItor = chunksLocation.iterator();

        while (chunkItor.hasNext()) {
            int chunkID = chunkItor.next();
            int location = locationItor.next();

            int chunkRemain = FileChunk.FIXED_SIZE - offset;
            char[] more;
            if (length >= chunkRemain) {
                more = readChunkData(location, fileName, chunkID, offset, chunkRemain);
            } else {
                more = readChunkData(location, fileName, chunkID, offset, length);
            }

            if (more != null) {
                result.append(more);
            } else {
                System.out.println("Failure");
                return null;
            }

            length -= FileChunk.FIXED_SIZE;
            offset = 0;
        }

        return result.toString();
    }

    /**
     * Write to chunk
     * @param data buffer
     * @param fileServerID file server ID
     * @param fileName file name
     * @param chunkID chunk ID for this file
     * @return size written if success, otherwise -1
     */
    private int writeChunkData(char[] data, int fileServerID, String fileName, int chunkID) {

        FileServer fileServer = allFileServerList.get(fileServerID);

        try {
            Socket fileSock = new Socket(fileServer.hostname, fileServer.requestFilePort);
            RequestEnvelop request = new RequestEnvelop("w", fileName);
            request.addParam(Integer.toString(chunkID));
            request.setData(data);

            ObjectOutputStream output = new ObjectOutputStream(fileSock.getOutputStream());
            output.writeObject(request);

            ObjectInputStream input = new ObjectInputStream(fileSock.getInputStream());
            ResponseEnvelop response = (ResponseEnvelop) input.readObject();

            if (Integer.valueOf(response.params.get(0)) == data.length) {
                return data.length;
            } else {
                return -1;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return 0;
    }

    /**
     * Write data to file servers according the information return by meta server
     * @param data buffer
     * @param fileName file name
     * @param chunks chunk ID list
     * @param chunkLocations file server list
     * @return size written if success, otherwise -1
     */
    public int writeData(String data, String fileName, List<Integer> chunks, List<Integer> chunkLocations) {
        char[] buffer = data.toCharArray();

        Iterator<Integer> chunkItor = chunks.iterator();
        Iterator<Integer> locationItor = chunkLocations.iterator();

        int start = 0;
        int end;
        while (chunkItor.hasNext()) {
            int chunkID = chunkItor.next();
            int location = locationItor.next();

            if (start + FileChunk.FIXED_SIZE >= buffer.length) {
                // only write non-null data
                end = buffer.length;
            } else {
                // cover a whole chunk
                end = start + FileChunk.FIXED_SIZE;
            }
            char[] dataToWrite = Arrays.copyOfRange(buffer, start, end);
            start += FileChunk.FIXED_SIZE;

            int ret = writeChunkData(dataToWrite, location, fileName, chunkID);
            if (ret < 0) {
                return -1;
            }
        }

        return data.length();
    }

    /**
     * Send request to meta server
     * @param request already initialized and set
     * @param clientSock established socket
     * @return 0 if success, -1 if fail
     */
    public int sendRequestToMeta(RequestEnvelop request, Socket clientSock) {
        try {
            ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());
            output.writeObject(request);
            output.flush();

        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        return 0;
    }

    /**
     * Receive response for latest request
     * @param clientSock established socket
     * @return response body
     */
    public ResponseEnvelop receiveResponseFromMeta(Socket clientSock) {
        ResponseEnvelop response = null;
        try {
            ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());

            response = (ResponseEnvelop) input.readObject();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return response;
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

    /**
     * Retrieve information about all file servers
     * @param doc XML
     */
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

    /**
     * Get hostname and port of meta
     * @param serverNode element node for meta
     */
    private void parseXMLToConfigMetaServer(Node serverNode) {
        NodeList serverConfig = serverNode.getChildNodes();

        for (int j = 0; j < serverConfig.getLength(); j++) {
            Node oneConfig = serverConfig.item(j);
            if (oneConfig.getNodeType() != Node.ELEMENT_NODE) {
                continue;
            }

            if (oneConfig.getNodeName().equals("hostname")) {
                this.metaHostName = oneConfig.getTextContent();
            }
            if (oneConfig.getNodeName().equals("clientPort")) {
                this.metaClientPort = Integer.parseInt(oneConfig.getTextContent());
            }
        }
    }


    public static void main(String[] args) {
        BufferedReader reader = null;
        if (args.length > 0) {
            try {
                reader = new BufferedReader(new FileReader(args[0]));
            } catch (FileNotFoundException e) {
                System.exit(0);
            }
        } else {
            reader = new BufferedReader(new InputStreamReader(System.in));
        }

        FileClient client = new FileClient("configs.xml");

        String line;
        try {
            while ((line = reader.readLine()) != null && !line.equals("")) {
                String[] params = line.split("\\|");

                int error = client.execute(params);
                if (error < 0) {
                    System.out.println("Failure: " + error);
                }

                Thread.sleep(2000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

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
    public static final int FILE_SERVER_NOT_AVAILABLE = -3;
    public static final int META_SERVER_NOT_AVAILABLE = -4;
    public static final int CAUSAL_ORDERING_VIOLATED = -5;
    public static final int FILE_NOT_EXIST = -6;
    public static final int FILE_LENGTH_EXCEED = -7;
    public static final int INVALID_COMMAND = -8;

    HashMap<Integer, FileServer> allFileServerList;

    //String metaHostName; // hostname of meta server
    //int metaClientPort; // port of meta server to receive client request

    MetaServer metaServer;

    //Socket clientSock;

    public FileClient(String xmlFile) {
        allFileServerList = new HashMap<>();
        parseXML(xmlFile);
        resolveMetaAddress();
        resolveAllFileServerAddress();
    }

    private void resolveMetaAddress() {
        metaServer.resolveAddress();
    }

    public int execute(String[] params) {
        int status = 0;

        Socket clientSock;
        try {
            //System.out.println("Client connect to meta server: " + metaHostName + ":" + metaClientPort);
            clientSock = new Socket(metaServer.metaServerAddress, metaServer.clientPort);
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
                request.addParam(Integer.toString(params[2].length())); // length
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
                } else {
                    System.out.println("Read failure");
                    status = -1;
                }

                break;
            case 'w':
                fileName = response.requestCopy.fileName;
                int ret = writeData(params[params.length - 1], fileName, response.chunksToScan, response.chunksLocation);
                if (ret == params[params.length - 1].length()) {
                    System.out.println("Write success");
                } else {
                    System.out.println("Failure");
                    status = -1;
                }

                break;
            case 'a':
                fileName = response.requestCopy.fileName;
                if (response.params == null || response.params.size() == 0) {
                    System.out.println("Chunk is not available");
                    status = -1;
                    break;
                }

                int ret1 = appendData(params[params.length - 1], fileName, Integer.valueOf(response.params.get(0)), response.chunksToScan, response.chunksLocation);
                if (ret1 == params[params.length - 1].length()) {
                    System.out.println("Append success");
                } else {
                    System.out.println("Failure");
                    status = -1;
                }

                break;
            default:
        }

        try {
            clientSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return status;
    }

    /**
     * Resolve all IP address of file servers
     */
    private void resolveAllFileServerAddress() {
        for (Map.Entry<Integer, FileServer> pair : allFileServerList.entrySet()) {
            pair.getValue().resolveAddress();
        }
    }

    /**
     * Read all data from chunk
     *
     * @param fileServerID file server ID
     * @param fileName     file name demanded
     * @param chunkID      chunk ID for the file
     * @return data if success, otherwise null
     */
    @Deprecated
    @SuppressWarnings("unused")
    private char[] readChunkData(int fileServerID, String fileName, int chunkID) {
        return readChunkData(fileServerID, fileName, chunkID, 0, FileChunk.FIXED_SIZE);
    }

    /**
     * Read data from chunk
     *
     * @param fileServerID file server ID
     * @param fileName     file name demanded
     * @param chunkID      chunk ID for the file
     * @param offset       offset inside the chunk
     * @param length       data length
     * @return data if success, otherwise null
     */
    private char[] readChunkData(int fileServerID, String fileName, int chunkID, int offset, int length) {
        FileServer fileServer = allFileServerList.get(fileServerID);
        if (fileServer == null) {
            return null;
        }

        try {
            Socket fileSock = new Socket(allFileServerList.get(fileServerID).fileServerAddress, fileServer.requestFilePort);
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

            if (response.error < 0) {
                return null;
            }
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
     *
     * @param fileName       demanded file
     * @param offset         offset in first chunk of returned
     * @param length         data length
     * @param chunksToScan   chunk list need to retrieve
     * @param chunksLocation file server ID
     * @return concatenated data
     */
    private String readData(String fileName, int offset, int length, List<Integer> chunksToScan, List<Integer> chunksLocation) {
        if (chunksLocation == null || chunksToScan == null) {
            return null;
        }

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
                //System.out.println("Failure");
                return null;
            }

            length -= FileChunk.FIXED_SIZE;
            offset = 0;
        }

        return result.toString();
    }

    /**
     * Write to chunk
     *
     * @param data         buffer
     * @param fileServerID file server ID
     * @param fileName     file name
     * @param chunkID      chunk ID for this file
     * @return size written if success, otherwise -1
     */
    private int writeChunkData(char[] data, int fileServerID, String fileName, int chunkID) {

        FileServer fileServer = allFileServerList.get(fileServerID);
        if (fileServer == null) {
            return -1;
        }
        if (data == null) {
            return 0;
        }

        try {
            Socket fileSock = new Socket(fileServer.fileServerAddress, fileServer.requestFilePort);
            RequestEnvelop request = new RequestEnvelop("w", fileName);
            request.addParam(Integer.toString(chunkID));
            request.setData(data);

            ObjectOutputStream output = new ObjectOutputStream(fileSock.getOutputStream());
            output.writeObject(request);
            output.flush();

            ObjectInputStream input = new ObjectInputStream(fileSock.getInputStream());
            ResponseEnvelop response = (ResponseEnvelop) input.readObject();

            input.close();
            fileSock.close();

            if (response.params.size() == 0) {
                return response.error != 0 ? response.error : -1;
            }

            if (Integer.valueOf(response.params.get(0)) == data.length) {
                return data.length;
            } else {
                return response.error != 0 ? response.error : -1;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return 0;
    }

    /**
     * Write data to file servers according the information return by meta server
     *
     * @param data           buffer
     * @param fileName       file name
     * @param chunks         chunk ID list
     * @param chunkLocations file server list
     * @return size written if success, otherwise -1
     */
    private int writeData(String data, String fileName, List<Integer> chunks, List<Integer> chunkLocations) {
        if (chunks == null || chunkLocations == null) {
            return -1;
        }

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
                System.out.println("Write error: " + ret);
                sendACKTOMeta(fileName, new ArrayList<>(chunks), false);
                return -1;
            }
        }

        return data.length();
    }

    /**
     * Append data to specified chunk, data in the chunk that exceed the remain space will be ignored
     *
     * @param data         to append
     * @param fileServerID file server ID
     * @param fileName     file name
     * @param chunkID      chunk ID in the file server in the chunk
     * @return actual size appended if success, otherwise -1
     */
    private int appendChunkData(char[] data, int fileServerID, String fileName, int chunkID) {
        FileServer fileServer = allFileServerList.get(fileServerID);
        if (fileServer == null) {
            return -1;
        }
        if (data == null) {
            return 0;
        }

        try {
            Socket fileSock = new Socket(fileServer.fileServerAddress, fileServer.requestFilePort);
            RequestEnvelop request = new RequestEnvelop("a", fileName);
            request.addParam(Integer.toString(chunkID));
            request.setData(data);

            ObjectOutputStream output = new ObjectOutputStream(fileSock.getOutputStream());
            output.writeObject(request);
            output.flush();

            ObjectInputStream input = new ObjectInputStream(fileSock.getInputStream());
            ResponseEnvelop response = (ResponseEnvelop) input.readObject();

            if (response.params == null || response.params.size() < 1) {
                return response.error != 0 ? response.error : -1;
            }

            if (Integer.valueOf(response.params.get(0)) == data.length) {
                return data.length;
            } else {
                return response.error != 0 ? response.error : -1;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return -1;
    }


    /**
     * Append data to file
     *
     * @param data           to append
     * @param fileName       file name
     * @param firstOffset    offset in first chunk, other chunk will be entirely new
     * @param chunks         chunk list
     * @param chunkLocations file server list
     * @return size written if success, otherwise -1
     */
    private int appendData(String data, String fileName, int firstOffset, List<Integer> chunks, List<Integer> chunkLocations) {
        if (chunks.size() == 0 || firstOffset < 0 || firstOffset >= FileChunk.FIXED_SIZE) {
            return 0;
        }

        char[] buffer = data.toCharArray();
        int bytesWritten = 0;

        Iterator<Integer> chunkItor = chunks.iterator();
        Iterator<Integer> locationItor = chunkLocations.iterator();

        // first chunk
        int chunkID = chunkItor.next();
        int location = locationItor.next();

        int ret;
        char[] dataToWrite = Arrays.copyOfRange(buffer, 0, Math.min(FileChunk.FIXED_SIZE - firstOffset, data.length()));

        if (firstOffset != 0) {
            // append
            ret = appendChunkData(dataToWrite, location, fileName, chunkID);
        } else {
            // write
            ret = writeChunkData(dataToWrite, location, fileName, chunkID);
        }

        if (ret < 0) {
            sendACKTOMeta(fileName, new ArrayList<>(chunks), false);
            System.out.println("Append error: " + ret);
            return -1;
        }
        bytesWritten += ret;

        // other chunks
        int start = bytesWritten;
        int end;
        while (chunkItor.hasNext()) {
            chunkID = chunkItor.next();
            location = locationItor.next();

            if (start + FileChunk.FIXED_SIZE >= buffer.length) {
                // only write non-null data
                end = buffer.length;
            } else {
                // cover a whole chunk
                end = start + FileChunk.FIXED_SIZE;
            }
            dataToWrite = Arrays.copyOfRange(buffer, start, end);
            start += FileChunk.FIXED_SIZE;

            ret = writeChunkData(dataToWrite, location, fileName, chunkID);
            if (ret < 0) {
                sendACKTOMeta(fileName, new ArrayList<>(chunks), false);
                System.out.println("Append error: " + ret);
                return ret;
            }
            bytesWritten += ret;
        }

        return bytesWritten;
    }

    /**
     * Send request to meta server
     *
     * @param request    already initialized and set
     * @param clientSock established socket
     * @return 0 if success, -1 if fail
     */
    private int sendRequestToMeta(RequestEnvelop request, Socket clientSock) {
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
     *
     * @param clientSock established socket
     * @return response body
     */
    private ResponseEnvelop receiveResponseFromMeta(Socket clientSock) {
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
     * Send ACK to meta server carrying with chunk information, and receive commit from server
     *
     * @param fileName  regarding this files
     * @param chunkList regarding this chunk
     * @param success   whether or not succeed to write operate this chunk
     * @return true if ACK send to meta and meta response commit. Otherwise false
     */
    private boolean sendACKTOMeta(String fileName, ArrayList<Integer> chunkList, boolean success) {

        try {
            Socket toMetaSock = new Socket(metaServer.metaServerAddress, metaServer.ackPort);

            ACKEnvelop ack = ACKEnvelop.clientAck(fileName, chunkList, success);
            ObjectOutputStream output = new ObjectOutputStream(toMetaSock.getOutputStream());
            output.writeObject(ack);
            output.flush();

            ObjectInputStream input = new ObjectInputStream(toMetaSock.getInputStream());
            ACKEnvelop ackFromMeta = (ACKEnvelop) input.readObject();

            input.close();

            if (ackFromMeta.type != ACKEnvelop.META_SERVER_ACK || ackFromMeta.ackNo != ack.ackNo) {
                return false;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        return true;
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
            //parseXMLToConfigMetaServer(metaServerNode);
            // config for file server virtual machine
            parseXMLToConfigFileServers(doc);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieve information about all file servers
     *
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
                if (params.length < 2) {
                    continue;
                }

                int tries = 3;
                do {
                    int error = client.execute(params);
                    if (error < 0) {
                        System.out.println("Failure: " + error + " , tries remaining: " + (tries - 1));
                    } else {
                        break;
                    }
                    Thread.sleep(2000);
                    System.out.println("Retry times " + (3 - tries + 1));
                } while ((--tries) > 0);

                if (tries == 0) {
                    System.out.println("Latest command failed");
                }

                Thread.sleep(2000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

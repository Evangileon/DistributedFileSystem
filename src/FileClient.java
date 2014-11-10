import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;


public class FileClient {
    //ArrayList<Socket> socketToServers;
    public static final int SUCCESS = 0;
    public static final int CHUNK_NOT_AVAILABLE = -1;
    public static final int FILE_SERVER_NOT_AVAILABLE = -2;
    public static final int META_SERVER_NOT_AVAILABLE = -3;
    public static final int CAUSAL_ORDERING_VIOLATED = -4;

    HashMap<Integer, FileServer> allFileServerList;

    String metaHostName; // hostname of meta server
    int metaClientPort; // port of meta server to receive client request

    //Socket clientSock;

    public FileClient(String xmlFile) {
        allFileServerList = new HashMap<>();
        parseXML(xmlFile);
    }

    public int execute(String[] params) {
        Socket clientSock = null;
        try {
            clientSock = new Socket(metaHostName, metaClientPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert clientSock != null : "Meta server is not available";

        RequestEnvelop request = new RequestEnvelop(params[0], params[1]);
        request.addParam(Arrays.copyOfRange(params, 2, params.length - 1));

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

        System.out.println("Response UUID: " + response.uuid.toString());

        return 0;
    }

    public int sendRequestToMeta(RequestEnvelop request, Socket clientSock) {
        try {
            ObjectOutputStream output = new ObjectOutputStream(clientSock.getOutputStream());

            output.writeObject(request);
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        return 0;
    }

    public ResponseEnvelop receiveResponseFromMeta(Socket clientSock) {
        ResponseEnvelop response = null;
        try {
            ObjectInputStream input = new ObjectInputStream(clientSock.getInputStream());

            response = (ResponseEnvelop)input.readObject();
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

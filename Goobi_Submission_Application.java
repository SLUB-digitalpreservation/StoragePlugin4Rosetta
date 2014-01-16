
/** used example from http://www.exlibrisgroup.org/display/RosettaOI/Submission+Application
 * adapted by Andreas Romeyke for GOOBI Workflow at SLUB Dresden
 */
import java.io.File;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument.DepositData;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositResultDocument.DepositResult;
import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.ProducerWebServices;
import com.exlibris.dps.ProducerWebServices_Service;
import com.exlibris.dps.SipStatusInfo;
import com.exlibris.dps.SipWebServices_Service;
import com.exlibris.dps.sdk.pds.PdsClient;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Properties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URL;
import javax.xml.namespace.QName;

public class Goobi_Submission_Application {

    static String userName = "admin1"; // rosetta default, will be overwritten
    static final String institution = "SLUB"; // default was "INS00" 
    static String password = "a12345678A"; // rosetta default, will be overwritten
    static String materialflowId = "66515"; // MF_Goobi_SFTP
    static final String depositSetId = "1";
    static String base_URL = "http://sdvrosetta-test.slub-dresden.de";
    //should be placed under where submission format of MF is configured
    static String subDirectoryName = "/DepositExample1";
    static final String folder_on_working_machine = "../dps-sdk-deposit/data/depositExamples";
    static final String filesRootFolder = folder_on_working_machine + "/DepositExample1/content/streams/";
    static final String IEfullFileName = folder_on_working_machine + "/DepositExample1/content/ie1.xml";
    static String PDS_URL = base_URL + ":8991/pds";
    static String DEPOSIT_WSDL_URL = base_URL + ":1801/dpsws/deposit/DepositWebServices?wsdl";
    static String PRODUCER_WSDL_URL = base_URL + ":1801/dpsws/backoffice/ProducerWebServices?wsdl";
    static String SIP_STATUS_WSDL_URL = base_URL + ":1801/dpsws/repository/SipWebServices?wsdl";
    static String SRUBASE = base_URL + ":1801/delivery/sru";
    static boolean with_dublette_check = false;
    static boolean use_this_host = false;
    static boolean cli_materialflow = false;

    private static void set_urls(String url) {
        // TODO: check given URL to avoid security outbreaks
        base_URL = url;
        PDS_URL = base_URL + ":8991/pds";
        DEPOSIT_WSDL_URL = base_URL + ":1801/dpsws/deposit/DepositWebServices?wsdl";
        PRODUCER_WSDL_URL = base_URL + ":1801/dpsws/backoffice/ProducerWebServices?wsdl";
        SIP_STATUS_WSDL_URL = base_URL + ":1801/dpsws/repository/SipWebServices?wsdl";
        SRUBASE = base_URL + "1801/delivery/sru";
    }

    /**
     * Full Flow Example with all stages to create and make a Deposit.
     *
     */
    public static void main(String[] args) {
        //org.apache.log4j.helpers.LogLog.setQuietMode(true);
        //for(int i=0; i<args.length; i++){
        //    System.out.println("arg " + i + " is:" + args[i]);
        //}

        int expected = 1;
        for (int x = 0; x < args.length; x++) {
            //System.out.println("x=" + x + " expected=" + expected + " len=" + args.length );
            if (args[x].equals("-h")) {
                System.out.println(args[x]);
                System.out.println(
                        "Goobi_Submission_Application [-h|--use-this-host|--host {hostname}|--materialflow {materialflow}]");
                return;
            } else if (args[x].equals("--use-this-host")) {
                expected++;
                use_this_host = true;
                String url = "";
                //set use_this_host
                try {
                  InetAddress addr = InetAddress.getLocalHost();
                  url = "http://" + addr.getHostAddress();
                } catch (UnknownHostException e) {
                    System.err.println("could not recognize localhost");
                    System.exit(-1);
                }
                System.out.println("using this url:'" + url + "'");
                set_urls(url);
            } else if (args[x].equals("--host")) {
                expected += 2;
                x++;
                String url = "http://" + args[x];
                System.out.println("using this url:'" + url + "'");
                set_urls(url);
            } else if (args[x].equals("--materialflow")) {
                expected += 2;
                x++;
                materialflowId = args[x];
                System.out.println("using this materialflow:'" + materialflowId + "'");
                cli_materialflow = true;
            } else {
                subDirectoryName = args[x];
            }
        } // endfor
        if ((args.length == 0) || (expected != args.length)) {
            System.out.println("needs following arguments: subDirectoryName");
            System.out.println("\tsubDirname is the name of the \"Vorgang\" without path,  ");
            System.out.println("\texample: '/home/rosetta/SFTP_INGEST/10008' contains 'content'");
            System.out.println("\t\tthen the subDirectoryname is '10008' :) ");
            System.out.println("but got  " + args.length + " arguments");
            System.exit(-1);
        }
        System.out.println("subDir=" + subDirectoryName);
        /* assert, that rights are only readable for this user */
        String propfile = System.getProperty("user.home") + "/.gsa";
        File fil = new File(propfile);
        if (!fil.exists()) {
            System.err.println("file '" + propfile + "' does not exists");
            System.exit(-1);
        }
        if (fil.canWrite()) {
            System.err.println("file '" + propfile + "' should not be writeable!");
            System.exit(-1);
        }
        if (!fil.canRead()) {
            System.err.println("file '" + propfile + "' should be readable");
            System.exit(-1);
        }
        if (!fil.isFile()) {
            System.err.println("file '" + propfile + "' is not a file");
            System.exit(-1);
        }
        /* read user and passwd from ".gsa" file */
        Properties properties = new Properties();
        try {
            BufferedInputStream stream = new BufferedInputStream(new FileInputStream(propfile));
            properties.load(stream);
            stream.close();
        } catch (IOException e) {
            System.err.println("Could not read file '" + propfile + "'");
            e.printStackTrace();
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        try {
            userName = properties.getProperty("user");
            password = properties.getProperty("password");
            if (cli_materialflow == false) {
                materialflowId = properties.getProperty("materialflowId");
            }
        } catch (SecurityException e) {
            System.err.println("submission application controlled by security manager, not allowed operation");
            System.exit(-1);
        } catch (NullPointerException e) {
            System.err.println("internal error, missed key calling properties.getProperty");
            System.exit(-1);
        } catch (IllegalArgumentException e) {
            System.err.println("internal error, key empty by calling properties.getProperty");
            System.exit(-1);
        }
        if (userName == null) {
            System.err.println("username in '" + propfile + "' should not be empty");
            System.exit(-1);
        }
        if (password == null) {
            System.err.println("password in '" + propfile + "' should not be empty");
            System.exit(-1);
        }
        if (materialflowId == null) {
            System.err.println("materialflowId in '" + propfile + "' should not be empty");
            System.exit(-1);
        }


        // 1. Create a SIP directory
        // 2. Create the IE using IE parser
        // 3. Place the SIP directory in a folder that can be accessed by the Rosetta application (using FTP is a valid approach)
        // 4. Authenticate using the PDS authentication API

        // Connecting to PDS
        PdsClient pds = PdsClient.getInstance();
        pds.init(PDS_URL, false);
        String pdsHandle = "";
        try {
            pdsHandle = pds.login(institution, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not login");
            System.exit(-1);
        }
        System.out.println("pdsHandle: " + pdsHandle);

        // 5. Submit Depo -1t
        try {
            ProducerWebServices producerWebServices = new ProducerWebServices_Service(new URL(PRODUCER_WSDL_URL), new QName("http://dps.exlibris.com/", "ProducerWebServices")).getProducerWebServicesPort();

            String producerAgentId = producerWebServices.getInternalUserIdByExternalId(userName);
            String xmlReply = producerWebServices.getProducersOfProducerAgent(producerAgentId);
            DepositDataDocument depositDataDocument = DepositDataDocument.Factory.parse(xmlReply);
            DepositData depositData = depositDataDocument.getDepositData();

            String producerId = depositData.getDepDataArray(0).getId();
            System.out.println("Producer ID: " + producerId);

            //submit

            String retval = new DepositWebServices_Service(new URL(DEPOSIT_WSDL_URL), new QName("http://dps.exlibris.com/", "DepositWebServices")).getDepositWebServicesPort().submitDepositActivity(pdsHandle, materialflowId, subDirectoryName, producerId, depositSetId);
            System.out.println("Submit Deposit Result: " + retval);

            DepositResultDocument depositResultDocument = DepositResultDocument.Factory.parse(retval);
            DepositResult depositResult = depositResultDocument.getDepositResult();

            // 6.check status of sip when deposit was successful
            Thread.sleep(3000);//wait until deposit is in
            if (depositResult.getIsError()) {
                System.out.println("Submit Deposit Failed");
                System.exit(1);
            } else {
                SipStatusInfo status = new SipWebServices_Service(new URL(SIP_STATUS_WSDL_URL), new QName("http://dps.exlibris.com/", "SipWebServices")).getSipWebServicesPort().getSIPStatusInfo(String.valueOf(depositResult.getSipId()));
                System.out.println("Submitted Subdirectory '" + subDirectoryName + "' becomes SIP '" + depositResult.getSipId() + "' ");
                System.out.println("Submitted Subdirectory '" + subDirectoryName + "' has DIP '" + depositResult.getDepositActivityId() + "' ");
                System.out.println("Submitted Deposit Status: " + status.getStatus());
                System.out.println("Submitted Deposit Stage: " + status.getStage());
                System.out.println("Submitted Deposit is in Module: " + status.getModule());
            }
        } catch (java.net.MalformedURLException e) {
            e.printStackTrace();
            System.err.println("malformed URL");
            System.exit(-1);
        } catch (java.lang.InterruptedException e) {
            e.printStackTrace();
            System.err.println("interrupted process");
            System.exit(-1);
        } catch (com.exlibris.dps.Exception_Exception e) {
            e.printStackTrace();
            System.err.println("unknown Exlibris error");
            System.exit(-1);
        } catch (org.apache.xmlbeans.XmlException e) {
            e.printStackTrace();
            System.err.println("apache XMLbeans error");
            System.exit(-1);
        }
        // logout
        try {
            pds.logout(pdsHandle);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not logout");
            System.exit(-1);
        }
        return;
    }
}


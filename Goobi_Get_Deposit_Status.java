/** used example from http://www.exlibrisgroup.org/display/RosettaOI/Submission+Application
 * adapted by Andreas Romeyke for GOOBI Workflow at SLUB Dresden
 */

import java.io.File;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositDataDocument.DepositData;
import com.exlibris.digitool.deposit.service.xmlbeans.DepositActivityListDocument;
import com.exlibris.dps.DepositWebServices_Service;
import com.exlibris.dps.ProducerWebServices;
import com.exlibris.dps.ProducerWebServices_Service;
import com.exlibris.dps.sdk.pds.PdsClient;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Properties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.URL;
import javax.xml.namespace.QName;
//import gov.loc.zing.srw.*;


public class Goobi_Get_Deposit_Status {
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
 
    static String PDS_URL             = base_URL + ":8991/pds";
    static String DEPOSIT_WSDL_URL    = base_URL + ":1801/dpsws/deposit/DepositWebServices?wsdl";
    static String PRODUCER_WSDL_URL   = base_URL + ":1801/dpsws/backoffice/ProducerWebServices?wsdl";
    static String SIP_STATUS_WSDL_URL = base_URL + ":1801/dpsws/repository/SipWebServices?wsdl";
    static String SRUBASE             = base_URL + ":1801/delivery/sru";
    static boolean use_this_host=false;
    
    private static void set_urls(String url) {
      // TODO: check given URL to avoid security outbreaks
      base_URL=url;
      PDS_URL=base_URL + ":8991/pds";
      DEPOSIT_WSDL_URL= base_URL + ":1801/dpsws/deposit/DepositWebServices?wsdl";
      PRODUCER_WSDL_URL= base_URL + ":1801/dpsws/backoffice/ProducerWebServices?wsdl";
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
      for(int x = 0; x < args.length; x++){
        if(args[x].equals("-h")){
          System.out.println(args[x]);
          System.out.println(
              "Goobi_Get_Deposit_Status [-h|--use-this-host|--host {hostname}]"
              );
          return;
        } else if(args[x].equals("--use-this-host")) {
          use_this_host=true;
          //set use_this_host
          InetAddress addr;
          try {
            addr = InetAddress.getLocalHost();
          } catch (UnknownHostException e) {
            System.err.println("could not recognize localhost");
            return;
          }
          String url="http://"+ addr.getHostAddress();
          System.out.println("using this url:'"+url+"'");
          set_urls(url);
        } else if (args[x].equals("--host")) {
        	x++;
        	String url="http://"+ args[x];
                System.out.println("using this url:'"+url+"'");
                set_urls(url); 
        }
      } // endfor
      /* assert, that rights are only readable for this user */
      String propfile=System.getProperty("user.home") + "/.gsa";
      File fil = new File(propfile);
      if (! fil.exists()) {
        System.err.println("file '"+propfile+"' does not exists");
        return;
      }
      if (fil.canWrite()) {
        System.err.println("file '"+propfile+"' should not be writeable!");
        return;
      }
      if (!fil.canRead()) {
        System.err.println("file '"+propfile+"' should be readable");
        return;
      }
      if (!fil.isFile()) {
        System.err.println("file '"+propfile+"' is not a file");
        return;
      }
      /* read user and passwd from ".gsa" file */
      Properties properties = new Properties();
      try {
        BufferedInputStream stream = new BufferedInputStream(new FileInputStream(propfile));
        properties.load(stream);
        stream.close();
        try {
          userName = properties.getProperty("user");
          password = properties.getProperty("password");
        } catch (SecurityException e) {
          System.err.println("submission application controlled by security manager, not allowed operation");
        } catch (NullPointerException e) {
          System.err.println("internal error, missed key calling properties.getProperty");
        } catch (IllegalArgumentException e) {
          System.err.println("internal error, key empty by calling properties.getProperty");
        }
        if (userName == null) {
          System.err.println("username in '"+propfile+"' should not be empty");
        }
        if (password  ==  null) {
          System.err.println("password in '"+propfile+"' should not be empty");
        }
        //if (materialflowId  ==  null) {
        //  System.err.println("materialflowId in '"+propfile+"' should not be empty");
        //}

        try {
          // 1. Create a SIP directory
          // 2. Create the IE using IE parser
          // 3. Place the SIP directory in a folder that can be accessed by the Rosetta application (using FTP is a valid approach)
          // 4. Authenticate using the PDS authentication API
          System.out.println("connect to '"+base_URL+"'");
          // Connecting to PDS
          PdsClient pds = PdsClient.getInstance();
          pds.init(PDS_URL,false);
          String pdsHandle = pds.login(institution, userName, password);
          System.out.println("pdsHandle: " + pdsHandle);

          // 5. Submit Depo -1t

          ProducerWebServices producerWebServices = new ProducerWebServices_Service(new URL(PRODUCER_WSDL_URL),new QName("http://dps.exlibris.com/", "ProducerWebServices")).getProducerWebServicesPort();
          String producerAgentId = producerWebServices.getInternalUserIdByExternalId(userName);
          String xmlReply = producerWebServices.getProducersOfProducerAgent(producerAgentId);
          DepositDataDocument depositDataDocument = DepositDataDocument.Factory.parse(xmlReply);
          DepositData depositData = depositDataDocument.getDepositData();

          String producerId = depositData.getDepDataArray(0).getId();
          System.out.println("Producer ID: " + producerId);

          // 6. Call Webservice

          String depositActivityStatus="All";
          String submitDateFrom="01/01/2013";
          String submitDateTo="01/01/2030";
          String startRecord="1";
          String endRecord="99999999";
          String retval = new DepositWebServices_Service(new URL(DEPOSIT_WSDL_URL),new QName("http://dps.exlibris.com/", "DepositWebServices")).getDepositWebServicesPort().getDepositActivityBySubmitDate(
          	pdsHandle,
          	depositActivityStatus,
          	producerId,
          	producerAgentId,
          	submitDateFrom,
          	submitDateTo,
          	startRecord,
          	endRecord
          	);
          System.out.println("Submit Deposit Result: " + retval);
          System.out.println("####################");

          DepositActivityListDocument depositActivityListDocument = DepositActivityListDocument.Factory.parse(retval);
          File file = new File ("list_of_deposits.xml");
          depositActivityListDocument.save(file);
          System.out.println("Result written to file 'list_of_deposits.xml'");
        } catch (Exception e) {
          e.printStackTrace();
          return;
        }
      }  catch (IOException e) {
        System.err.println("Could not read file '"+propfile+"'");
        e.printStackTrace();
        return;

      }catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }
}  

package org.slub.rosetta.dps.repository.plugin.storage.nfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import com.exlibris.dps.repository.plugin.storage.nfs.NFSStoragePlugin;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.digitool.common.storage.Fixity;
import com.exlibris.digitool.infrastructure.utils.Checksummer;
import com.exlibris.digitool.common.dnx.DnxDocument;
 
import java.util.HashMap;
import org.slub.rosetta.dps.repository.plugin.storage.nfs.SLUBStoragePlugin;
import java.io.File;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;

/**
 * Tests for {@link org.slub.rosetta.dps.repository.plugin.storage.nfs.SLUBStoragePlugin}.
 *
 * @author andreas.romeyke@slub-dresden.de (Andreas Romeyke)
 */
@RunWith(JUnit4.class)
public class testSLUBStoragePlugin {

  @Test
  public void helloWorld() {
    System.out.println("Hallo Welt!");
    assertTrue("helloworld test", true );
  }

  @Test
  public void getStreamDirectory() {
    SLUBStoragePlugin mock = new SLUBStoragePlugin();
    HashMap <String, String> map = new HashMap<String,String> ();
    map.put("DIR_ROOT", "./");
    map.put("DIR_PREFIX", "./");
    map.put("FILES_HANDLING_METHOD", "move");
    mock.setParameters( map );
    // TODO:
    File calced_path = mock.getStreamDirectory("foo", "bar");
    System.out.println("getStreamDirectory:"+ calced_path.getName()+ "\n");
    assertTrue("getStreamDirectory test", true );
    
  }

  @Test
  public void getStreamRelativePath() throws Exception {
    SLUBStoragePlugin mock = new SLUBStoragePlugin();
    HashMap <String, String> map = new HashMap<String,String> ();
    map.put("DIR_ROOT", "./");
    map.put("DIR_PREFIX", "./");
    map.put("FILES_HANDLING_METHOD", "move");
    mock.setParameters( map );
    // TODO:
    StoredEntityMetaData storedEntityMetaData = new StoredEntityMetaData();
    storedEntityMetaData.setEntityType(StoredEntityMetaData.EntityType.IE);
    storedEntityMetaData.setCurrentFilePath("foo");
    storedEntityMetaData.setEntityPid("IE00000");
    DnxDocument iedoc = null;
    iedoc.setDocumentXml("   	<mets:mets xmlns:mets='http://www.loc.gov/METS/'>     	<mets:dmdSec ID='ie-dmd'>    	<mets:mdWrap MDTYPE='DC'>      <mets:xmlData>        <dc:record xmlns:dc='http://purl.org/dc/elements/1.1/'>          <dc:identifier>slub:goobi:vorgang:10008</dc:identifier>          <dc:coverage>DE-14</dc:coverage>          <dc:coverage>Hist.Sax.M.37.t,120</dc:coverage>          <dc:relation>Saxonica</dc:relation>          <dc:identifier>oai:de:slub-dresden:db:id-319037843</dc:identifier>          <dc:format>[1] Bl.</dc:format>          <dc:identifier>319037843</dc:identifier>          <dc:identifier>http://digital.slub-dresden.de/id319037843</dc:identifier>          <dc:identifier>urn:nbn:de:bsz:14-db-id3190378431</dc:identifier>          <dc:identifier>266278965</dc:identifier>          <dc:title>Eingabe der Handelskammer zu Leipzig den Entwurf eines Tabak-Steuer-Gesetzes betr.</dc:title>          <dc:language>de</dc:language>          <dc:date>1893</dc:date>          <dc:subject>eingdehaz</dc:subject>        </dc:record>      </mets:xmlData>    </mets:mdWrap>  </mets:dmdSec>");    storedEntityMetaData.setIeDnx(iedoc);
    	mock.getStreamRelativePath( storedEntityMetaData);
  }
  
  @Test
  @Ignore("Test is ignored for testing purposes")
  public void thisIsIgnored() {
  }
}

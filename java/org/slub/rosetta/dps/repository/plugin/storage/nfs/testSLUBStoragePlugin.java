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
 
import java.util.HashMap;
import org.slub.rosetta.dps.repository.plugin.storage.nfs.SLUBStoragePlugin;
import java.io.File;
/**
 * Tests for {@link org.slub.rosetta.dps.repository.plugin.storage.nfs.SLUBStoragePlugin}.
 *
 * @author andreas.romeyke@slub-dresden.de (Andreas Romeyke)
 */
@RunWith(JUnit4.class)
public class testSLUBStoragePlugin {

  @Test
  public void thisAlwaysPasses() {
    System.out.println("Hallo Welt!");
    assertTrue("helloworld test", true );
  }

  @Test
  public void thisAlwaysPasses2() {
    SLUBStoragePlugin mock = new SLUBStoragePlugin();
    HashMap <String, String> map = new HashMap<String,String> ();
    map.put("DIR_Root", "./");
    mock.setParameters( map );
    // TODO:
    File calced_path = mock.getStreamDirectory("foo", "bar");
    System.out.println("getStreamDirectory:"+ calced_path.getName()+ "\n");
    assertTrue("getStreamDirectory test", true );
    
  }

  @Test
  @Ignore("Test is ignored for testing purposes")
  public void thisIsIgnored() {
  }
}

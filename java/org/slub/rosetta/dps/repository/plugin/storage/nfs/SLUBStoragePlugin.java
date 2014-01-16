//package com.exlibris.dps.repository.plugin.storage.nfs;
package org.slub.rosetta.dps.repository.plugin.storage.nfs;
 
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import com.exlibris.dps.repository.plugin.storage.nfs.NFSStoragePlugin;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.digitool.common.storage.Fixity;
import com.exlibris.digitool.infrastructure.utils.Checksummer;
 
public class SLUBStoragePlugin extends NFSStoragePlugin {
 
    private static final String DIR_ROOT = "DIR_ROOT";
 
    public SLUBStoragePlugin() {
        super();
    }
 
 
    @Override
    public String storeEntity(InputStream is, StoredEntityMetaData storedEntityMetadata) throws Exception {
 
        String fileName = createFileName(storedEntityMetadata);
 
        String relativeDirectoryPath = getStreamRelativePath(storedEntityMetadata);
        File destFile = getStreamDirectory(relativeDirectoryPath, fileName);
 
        // better move/link
        if (canHandleSourcePath(storedEntityMetadata.getCurrentFilePath())) {
            is.close(); // close input stream so that 'move' can work, we don't use it anyway
            copyStream(storedEntityMetadata.getCurrentFilePath(), destFile.getAbsolutePath());
        }
        // default way - copy from input stream
        else {
            IOUtil.copy(is, new FileOutputStream(destFile));
        }
        String storedEntityIdentifier = relativeDirectoryPath + File.separator + fileName;
 
        if(!checkFixity(storedEntityMetadata.getFixities(), storedEntityIdentifier)) {
            deleteEntity(storedEntityIdentifier); // delete corrupt files
            return null;
        }
 
        // return only relative (not absolute) path
        return storedEntityIdentifier;
    }
 
 
    private String getStreamRelativePath(StoredEntityMetaData storedEntityMetaData ) throws Exception {
 
        String relativeDirectoryPath = File.separator;
        String pid = storedEntityMetaData.getEntityPid();
        Checksummer checksummer = new Checksummer(pid, true, false, false);
        Fixity fixity = new Fixity(Fixity.FixityAlgorithm.MD5.toString(), checksummer.getMD5());
        String value = fixity.getValue();
 
        for(int i=0 ; i<value.length() && i<32 ; i= i+2) {
            if(i+1 < value.length()) {
                relativeDirectoryPath += value.substring(i, i+2) + File.separator;
            } else {
                relativeDirectoryPath += value.substring(i, i+1) + File.separator;
            }
        }
 
        if(32 < value.length()) {
            relativeDirectoryPath += value.substring(32);
        }
 
        return relativeDirectoryPath;
    }
 
    private File getStreamDirectory(String path, String fileName) {
 
        File newDir = new File(parameters.get(DIR_ROOT) + File.separator + path);
        newDir.mkdirs();
        return new File(newDir.getAbsolutePath() + File.separator + fileName);
    }
 
    private boolean canHandleSourcePath(String srcPath) {
        try {
            File file = new File(srcPath);
            return file.canRead();
        }
        catch (Exception e) {
            return false;
        }
    }
 
}

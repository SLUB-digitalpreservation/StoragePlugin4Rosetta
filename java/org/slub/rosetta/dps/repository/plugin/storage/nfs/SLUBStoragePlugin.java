package org.slub.rosetta.dps.repository.plugin.storage.nfs;
 
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Calendar;
import java.text.SimpleDateFormat;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.digitool.common.dnx.DnxDocument;
import com.exlibris.digitool.common.storage.Fixity;
import com.exlibris.digitool.infrastructure.utils.Checksummer;
import com.exlibris.dps.repository.plugin.storage.nfs.NFSStoragePlugin;

/**
 * SLUBStoragePlugin
 * writes all IEs, files and so on into the same dir under yyyy/mm/dd/IEpid/
 *
 * @author andreas.romeyke@slub-dresden.de (Andreas Romeyke)
 */
public class SLUBStoragePlugin extends NFSStoragePlugin {
    private static final String DIR_ROOT = "DIR_ROOT";
    private static final ExLogger log = ExLogger.getExLogger(SLUBStoragePlugin.class);
    public SLUBStoragePlugin() {
        super();
        log.info("SLUBStoragePlugin instantiated");
    }
 
    @Override
    public String storeEntity(InputStream is, StoredEntityMetaData storedEntityMetadata) throws Exception {
        log.info("SLUBStoragePlugin.storeEntity() called");
        String fileName = createFileName(storedEntityMetadata);
        log.info("SLUBStoragePlugin.storeEntity() fileName="+fileName);
        String relativeDirectoryPath = getStreamRelativePath(storedEntityMetadata);
        log.info("SLUBStoragePlugin.storeEntity() relativeDirectoryPath="+relativeDirectoryPath);
        File destFile = getStreamDirectory(relativeDirectoryPath, fileName);
        log.info("SLUBStoragePlugin.storeEntity() destfile.getAbsolutePath()="+destFile.getAbsolutePath());
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
        log.info("SLUBStoragePlugin.storeEntity() storedEntityIdentifier="+storedEntityIdentifier);
        if(!checkFixity(storedEntityMetadata.getFixities(), storedEntityIdentifier)) {
            deleteEntity(storedEntityIdentifier); // delete corrupt files
            return null;
        }
        // return only relative (not absolute) path
        return storedEntityIdentifier;
    }
 
    // path should be of form yyyy/mm/dd/IE-PID/ 
    protected String getStreamRelativePath(StoredEntityMetaData storedEntityMetaData ) throws Exception {
        String relativeDirectoryPath = File.separator;
        // get IE PID by calling IE-DNX record and search for ""internalIdentifierType" == "PID"
        DnxDocument iedoc = storedEntityMetaData.getIeDnx();
        String iepid = iedoc.getSectionKeyValue("internalIdentifierType", "PID");
        log.info("SLUBStoragePlugin.getStreamRelativePath iepid=" + iepid);
        String datestring = iedoc.getSectionKeyValue("objectCharacteristics", "creationDate");
        Calendar date = Calendar.getInstance();
        // date: 2014-01-15 14:28:01
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        date.setTime(sdf.parse(datestring));
        log.info("SLUBStoragePlugin.getStreamRelativePath creation Date read=" + datestring + " parsed=" + date.toString());
        relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("yyyy").format(date);
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("MM").format(date);
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("dd").format(date);
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        relativeDirectoryPath = relativeDirectoryPath + iepid;
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        log.info("SLUBStoragePlugin.getStreamRelativePath relativeDirectoryPath=" + relativeDirectoryPath);
        return relativeDirectoryPath;
    }
 
    protected File getStreamDirectory(String path, String fileName) {
        File newDir = new File(parameters.get(DIR_ROOT) + File.separator + path);
        log.info("SLUBStoragePlugin.getStreamDirectory path=" + path);
        log.info("SLUBStoragePlugin.getStreamDirectory fileName=" + fileName);
        log.info("SLUBStoragePlugin.getStreamDirectory newDir.getAbsolutePath()=" + newDir.getAbsolutePath());
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

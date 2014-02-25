package org.slub.rosetta.dps.repository.plugin.storage.nfs;
 
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Iterator;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.digitool.common.dnx.DnxDocument;
import com.exlibris.digitool.common.dnx.DnxSection;
import com.exlibris.digitool.common.dnx.DnxSectionRecord;
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
 
    /** copied from NFS Storage Plugin, enhanced with debugging info */
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
 
    /** prepare right path
      * path should be of form yyyy/MM/dd/IE-PID/
      * we need to findout the associated dnx document (IE),
      * get the creation date of the IE/SIP
      * and get the IE PID of the associated IE.
      * returns the path as string 
      */
    protected String getStreamRelativePath(StoredEntityMetaData storedEntityMetaData ) throws Exception {
    	log.info("SLUBStoragePlugin AAB");
        String relativeDirectoryPath = File.separator;
        // get IE PID by calling IE-DNX record and search for ""internalIdentifierType" == "PID"
        
        DnxDocument iedoc = storedEntityMetaData.getIeDnx();
        String entitytype = storedEntityMetaData.getEntityType().name();
        DnxSection iesec = iedoc.getSectionById("internalIdentifier");
        if (null == iesec) {
        	log.error ("SLUBStoragePlugin.getStreamRelativePath no section in entity of type "+entitytype +" with 'internalIdentfier' in associated iedoc found, do you use plugin for others than permanent data? You should not!");
        	throw new Exception("error, no section in entity of type "+entitytype +" with 'internalIdentfier' in associated iedoc found, do you use plugin for others than permanent data? You should not!");
        }
        String iepid = null;
        log.info ("SLUBStoragePlugin.getStreamRelativePath iesec="+iesec.toString() );
        List<DnxSectionRecord> records = iesec.getRecordList();
        for (Iterator<DnxSectionRecord> iter = records.iterator(); iter.hasNext(); ) {
        	DnxSectionRecord element = iter.next();
        	if (element.getKeyById("internalIdentifierType").getValue().equals("PID")) {
        		iepid = element.getKeyById("internalIdentifierValue").getValue();
        		break;
        	}
        }
        // raise Exception if iepid is null
        if (null == iepid) {
        	log.error ("SLUBStoragePlugin.getStreamRelativePath iesec="+iesec.toString() );
        	throw new Exception("error, could not get IEPID for storedEntityMetaData:"+storedEntityMetaData.toString() +" of type " + entitytype);
        }
        log.info("SLUBStoragePlugin.getStreamRelativePath iepid=" + iepid + " (entitytype="+ entitytype +")");
        // get creationDate of "objectCharacteristics"
        String datestring = iedoc.getSectionKeyValue("objectCharacteristics", "creationDate");
        Calendar date = Calendar.getInstance();
        // date ist there stored in format (example): 2014-01-15 14:28:01
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setLenient(false); /* if parse errors, do not guess about */
        Date d = sdf.parse(datestring);
        date.setTime(d);
        log.info("SLUBStoragePlugin.getStreamRelativePath creation Date read=" + datestring + " parsed=" + date.toString());
        relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("yyyy").format(d);
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("MM").format(d);
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("dd").format(d);
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        relativeDirectoryPath = relativeDirectoryPath + iepid;
        relativeDirectoryPath = relativeDirectoryPath + File.separator;
        log.info("SLUBStoragePlugin.getStreamRelativePath relativeDirectoryPath=" + relativeDirectoryPath);
        return relativeDirectoryPath;
    }
 
    /** copied from NFS Storage Plugin, enhanced with debugging info */
    protected File getStreamDirectory(String path, String fileName) {
        File newDir = new File(parameters.get(DIR_ROOT) + File.separator + path);
        log.info("SLUBStoragePlugin.getStreamDirectory path=" + path);
        log.info("SLUBStoragePlugin.getStreamDirectory fileName=" + fileName);
        log.info("SLUBStoragePlugin.getStreamDirectory newDir.getAbsolutePath()=" + newDir.getAbsolutePath());
        boolean arecreated = newDir.mkdirs();
        log.info("SLUBStoragePlugin.getStreamDirectory newDir.mkdirs(), directories are created:" + arecreated);
        return new File(newDir.getAbsolutePath() + File.separator + fileName);
    }
 
    /** copied from NFS Storage Plugin, enhanced with debugging info */
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

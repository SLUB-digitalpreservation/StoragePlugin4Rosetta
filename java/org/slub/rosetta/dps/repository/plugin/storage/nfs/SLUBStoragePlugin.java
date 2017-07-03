/*

2014-2016 by Andreas Romeyke (SLUB Dresden)

The code contributed by SLUB is licensed under apache 2.0 and based partially
on NFS Storage Plugin, 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.slub.rosetta.dps.repository.plugin.storage.nfs;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.infra.svc.api.scriptRunner.ExecExternalProcess;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.core.sdk.storage.handler.AbstractStorageHandler;
import com.exlibris.core.sdk.storage.handler.StorageUtil;
import com.exlibris.core.sdk.utils.FileUtil;
import com.exlibris.digitool.common.dnx.DnxDocument;
import com.exlibris.digitool.common.dnx.DnxSection;
import com.exlibris.digitool.common.dnx.DnxSectionRecord;
import com.exlibris.digitool.common.storage.Fixity;
import com.exlibris.digitool.infrastructure.utils.Checksummer;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * SLUBStoragePlugin
 * writes all IEs, files etc. into the same directory under yyyy/mm/dd/IEpid/
 *
 * HINT: this plugin is *only* useful to store AIPs (IEs + files) in
 *       permanent storage. Otherwise (e.g in the case of SIPs) it can't
 *       detect the IE-PID and will report an error.
 *
 * the fixities will only be checked if files are stored (storeEntity()), 
 * because the Rosetta API does not give enough information about available
 * fixities by calling retrieveEntity() to read files, because only 
 * a string containing the path is given. 
 *
 * Please note, that all undocumented codelines are in most cases copied from
 * com.exlibris.dps.repository.plugin.storage.nfs.NFSStoragePlugin and I am
 * unable to find full documentation.
 *
 * @author andreas.romeyke@slub-dresden.de (Andreas Romeyke)
 * @ see com.exlibris.dps.repository.plugin.storage.nfs.NFSStoragePlugin
 */
public class SLUBStoragePlugin extends AbstractStorageHandler {
    private static final ExLogger log = ExLogger.getExLogger(SLUBStoragePlugin.class);
    private static final String DIR_PREFIX = "DIR_PREFIX";
    private static final String DIR_ROOT = "DIR_ROOT"; /** {@inheritDoc} */
    private static final String FILE_PER_DIR = "FILE_PER_DIR";
    private static final String FILES_HANDLING_METHOD = "FILES_HANDLING_METHOD";
    private final String RELATIVE_DIRECTORY_PATH = "relativeDirectoryPath";
    private final String DEST_FILE_PATH = "destFilePath";

    public SLUBStoragePlugin() {
        log.info("SLUBStoragePlugin instantiated");
    }

    public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier)
            throws Exception
    {
        log.info("SLUBStoragePlugin.checkFixity()");
        return checkFixity(fixities, storedEntityIdentifier, true);
    }
    public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier, boolean isRelativePath)
            throws Exception
    {
        log.info("SLUBStoragePlugin.checkFixity() storedEntityIdentifier='" + storedEntityIdentifier + "' isRelativePath=" + isRelativePath);
        boolean result = true;
        if (fixities != null)
        {
            InputStream is = null;
            try {
                is = retrieveEntity(storedEntityIdentifier, isRelativePath);
                Checksummer checksummer = new Checksummer(is, true, true, true);
                for (Fixity fixity : fixities) {
                    fixity.setResult(false);
                    log.info("SLUBStoragePlugin.checkFixity() getAlgorithm=" + fixity.getAlgorithm());
                    log.info("SLUBStoragePlugin.checkFixity() FixityAlgorithm.MD5=" + Fixity.FixityAlgorithm.MD5.toString());
                    log.info("SLUBStoragePlugin.checkFixity() FixityAlgorithm.SHA1=" + Fixity.FixityAlgorithm.SHA1.toString());
                    log.info("SLUBStoragePlugin.checkFixity() FixityAlgorithm.SHA256=" + Fixity.FixityAlgorithm.SHA256.toString());
                    log.info("SLUBStoragePlugin.checkFixity() FixityAlgorithm.CRC32=" + Fixity.FixityAlgorithm.CRC32.toString());
                /* TODO: with upcoming versions of Rosetta, recheck need of
                 * Workaround for fixity.getAlgorithm() */
                    String algorithm = fixity.getAlgorithm().toUpperCase(); // workaround, because fixity.getAlgorithm() returns lowercase string
                    if (
                            (!Fixity.FixityAlgorithm.MD5.toString().equals(algorithm)) &&
                            (!Fixity.FixityAlgorithm.SHA1.toString().equals(algorithm)) &&
                            (!Fixity.FixityAlgorithm.SHA256.toString().equals(algorithm)) &&
                            (!Fixity.FixityAlgorithm.CRC32.toString().equals(algorithm))
                    ) {
                        result = checkFixityByPlugin(fixity, storedEntityIdentifier, isRelativePath, result);
                    } else {
                        log.info("SLUBStoragePlugin.checkFixity() calcMD5|calcSHA1|calcCRC32|calcSHA256=true");
                        int checksummerAlgorithmIndex = getChecksummerAlgorithmIndex(algorithm);
                        log.info("SLUBStoragePlugin.checkFixity() checksummerAlgorithmIndex=" + checksummerAlgorithmIndex);
                        if (checksummerAlgorithmIndex != -1)
                            result = checkFixityByBuiltin(fixity, algorithm, checksummer, result);
                    }
                }
            }
            finally
            {
                log.info("SLUBStoragePlugin.checkFixity() finally called");
                if (is != null) {
                    log.info("SLUBStoragePlugin.checkFixity()is closed");
                    is.close();
                }
            }
        }
        return result;
    }

    /** uses a checksummer object and check if fixity of given builtin algorithm is correct
     *
     * @param fixity concrete fixity object
     * @param algorithm which algorithm should be used
     * @param checksummer checksummer object of given input stream
     * @param result result is boolean variable to hold states of all fixity checks
     * @return result
     * @throws NoSuchAlgorithmException
     */
    private boolean checkFixityByBuiltin(Fixity fixity, String algorithm, Checksummer checksummer, boolean result) throws NoSuchAlgorithmException {
        String oldValue = fixity.getValue();
        log.info("SLUBStoragePlugin.checkFixity() getAlgorithm (2)=" + algorithm);
        log.info("SLUBStoragePlugin.checkFixity() oldvalue=" + oldValue);
        fixity.setValue(checksummer.getChecksum(algorithm));
        log.info("SLUBStoragePlugin.checkFixity() newvalue=" + fixity.getValue());
        fixity.setResult((oldValue == null) || (oldValue.equalsIgnoreCase(fixity.getValue())));
        result &= fixity.getResult().booleanValue();
        log.info("SLUBStoragePlugin.checkFixity() result=" + result);
        return result;
    }

    /** check fixity by calling its registered plugin
     *
     * @param fixity concrete fixity object
     * @param storedEntityIdentifier path to file which should be checked
     * @param isRelativePath indicates if path is relative
     * @param result result is boolean variable to hold states of all fixity checks
     * @return result
     * @throws Exception
     */
    private boolean checkFixityByPlugin(Fixity fixity, String storedEntityIdentifier, boolean isRelativePath, boolean result) throws Exception {
        log.info("SLUBStoragePlugin.checkFixity() another fixity");
        log.info("SLUBStoragePlugin.checkFixity() pluginname=" + fixity.getPluginName());
        String oldValue = "";
        try {
            oldValue = fixity.getValue();
            log.info("SLUBStoragePlugin.checkFixity() oldvalue=" + oldValue);
        }
        catch (Exception e) {
            log.warn("SLUBStoragePlugin failed to get previous fixity value ", e.getMessage());
        }
        fixity.setValue(getChecksumUsingPlugin(isRelativePath ? getLocalFilePath(storedEntityIdentifier) : storedEntityIdentifier, fixity.getPluginName(), oldValue));
        fixity.setResult((oldValue == null) || (oldValue.equals(fixity.getValue())));
        log.info("SLUBStoragePlugin.checkFixity() newvalue=" + fixity.getValue());
        result &= fixity.getResult().booleanValue();
        log.info("SLUBStoragePlugin.checkFixity() result=" + result);
        return result;
    }

    public boolean deleteEntity(String storedEntityIdentifier)
    {
        log.info("SLUBStoragePlugin.deleteEntity() storedEntityIdentifier='" + storedEntityIdentifier + "'");
        File file = new File(this.parameters.get("DIR_ROOT") + storedEntityIdentifier);
        try
        {
            return file.delete();
        }
        catch (Exception e)
        {
            log.warn("SLUBStoragePlugin failed to delete entity with path: " + file.getPath(), e.getMessage());
        }
        return true;
    }


    public String getLocalFilePath(String storedEntityIdentifier)
    {
        log.info("SLUBStoragePlugin.getLocalFilePath() with '" + storedEntityIdentifier + "'");
        return getFullFilePath(storedEntityIdentifier);
    }
    public String getFullFilePath(String storedEntityIdentifier)
    {
        log.info("SLUBStoragePlugin.getFullFilePath() with '" + storedEntityIdentifier + "'");
        return this.parameters.get("DIR_ROOT") + storedEntityIdentifier;
    }

    public InputStream retrieveEntity(String storedEntityIdentifier)
            throws IOException
    {
        log.info("SLUBStoragePlugin.retrieveEntity() with '" + storedEntityIdentifier + "'");
        return retrieveEntity(storedEntityIdentifier, true);
    }
    public InputStream retrieveEntity(String storedEntityIdentifier, boolean isRelative)
            throws IOException
    {
        log.info("SLUBStoragePlugin.retrieveEntity() with '" + storedEntityIdentifier + "' isrelative=" + isRelative);
        return new FileInputStream((isRelative ? this.parameters.get("DIR_ROOT") : "") + storedEntityIdentifier);
    }
    public byte[] retrieveEntityByRange(String storedEntityIdentifier, long start, long end)
            throws Exception
    {
        log.info("SLUBStoragePlugin.retrieveEntitybyRange() with '" + storedEntityIdentifier + "' start=" + start + "end=" + end);
        byte[] bytes = new byte[(int)(end - start + 1L)];
        RandomAccessFile file = null;
        try
        {
            file = new RandomAccessFile(this.parameters.get("DIR_ROOT") + storedEntityIdentifier, "r");
            file.seek(start);
            file.readFully(bytes, 0, (int)(end - start + 1L));
            return bytes;
        }
        finally
        {
            if (file != null) {
                try
                {
                    file.close();
                }
                catch (Exception e)
                {
                    log.warn("Failed closing file", e.getMessage());
                }
            }
        }
    }
    public String storeEntity(InputStream is, StoredEntityMetaData storedEntityMetadata)
            throws Exception
    {
        log.info("SLUBStoragePlugin.storeEntity()");
        String existsDescPath = getFilePathInDescIfExists(storedEntityMetadata);
        log.debug("SLUBStoragePlugin.storeEntity() existsDescPath='" + existsDescPath + "'");
        String destFilePath = null;

        boolean isCopyFileNeeded = true;
        if (existsDescPath != null)
        {
            destFilePath = existsDescPath;
            isCopyFileNeeded = !checkFixity(storedEntityMetadata.getFixities(), destFilePath, false);
        }
        log.debug("SLUBStoragePlugin.storeEntity() destFilePath='" + destFilePath + "'");
        Map<String, String> paths = getStoreEntityIdentifier(storedEntityMetadata, destFilePath);
        String storedEntityIdentifier = paths.get("relativeDirectoryPath");
        log.debug("SLUBStoragePlugin.storeEntity() storedEntityIdentifier='" + storedEntityIdentifier + "'");
        destFilePath = paths.get("destFilePath");
        log.debug("SLUBStoragePlugin.storeEntity() destFilePath (2)='" + destFilePath + "'");
        log.debug("SLUBStoragePlugin.storeEntity() isCopyFileNeeded='" + isCopyFileNeeded + "'");
        if (isCopyFileNeeded)
        {
            if (canHandleSourcePath(storedEntityMetadata.getCurrentFilePath()))
            {
                log.debug("SLUBStoragePlugin.storeEntity() destFilePath canhandle sourcepath");
                if (is != null) {
                    is.close();
                }
                copyStream(storedEntityMetadata, destFilePath);
            }
            else
            {
                log.debug("SLUBStoragePlugin.storeEntity() Cannot handle source path: " + storedEntityMetadata.getCurrentFilePath());
                if (is == null)
                {
                    log.warn("SLUBStoragePlugin.storeEntity() InputStream is null");
                    return null;
                }
                FileOutputStream output = null;
                try
                {
                    output = new FileOutputStream(new File(destFilePath));
                    IOUtil.copy(is, output);
                    log.debug("SLUBStoragePlugin.storeEntity() try copy was successfull");
                }
                finally
                {
                    IOUtil.closeQuietly(output);
                }
            }
            if (!checkFixity(storedEntityMetadata.getFixities(), storedEntityIdentifier)) {
                log.info("SLUBStoragePlugin.storeEntity() checkFixity failed");
                return null;
            }
        }
        log.debug("SLUBStoragePlugin.storeEntity() storedEntityIdentifier (2)='" + storedEntityIdentifier + "'");
        return storedEntityIdentifier;
    }
    protected void copyStream(StoredEntityMetaData storedEntityMetadata, String destPath)
            throws IOException
    {
        log.info("SLUBStoragePlugin.copyStream()");
        String filesHandlingMethod = this.parameters.get("FILES_HANDLING_METHOD");
        String srcPath = storedEntityMetadata.getCurrentFilePath();
        log.debug("SLUBStoragePlugin.copyStream() destPath='" + destPath + "'");
        log.debug("SLUBStoragePlugin.copyStream() srcPath='" + srcPath + "'");
        log.debug("SLUBStoragePlugin.copyStream() filesHandlingMethod='" + filesHandlingMethod + "'");
        String pid = storedEntityMetadata.getEntityPid();
        log.debug("SLUBStoragePlugin.copyStream() pid='" + pid + "'");
        String iePid = storedEntityMetadata.getIePid();
        log.debug("SLUBStoragePlugin.copyStream() iePid='" + iePid + "'");
        if ("move".equalsIgnoreCase(filesHandlingMethod))
        {
            File canonicalSrcFile = getCanonicalFile(srcPath);
            FileUtil.moveFile(canonicalSrcFile, new File(destPath));
            saveDestPathsTmpFile(iePid, pid, destPath);
        }
        else if ("soft_link".equalsIgnoreCase(filesHandlingMethod))
        {
            softLink(srcPath, destPath);
        }
        else if ("hard_link".equalsIgnoreCase(filesHandlingMethod))
        {
            hardLink(srcPath, destPath);
        }
        else
        {
            FileUtil.copyFile(srcPath, destPath);
            saveDestPathsTmpFile(iePid, pid, destPath);
        }
    }
    protected String getFilePathInDescIfExists(StoredEntityMetaData storedEntityMetadata)
    {
        log.info("SLUBStoragePlugin.getFilePathInDescIfExists()");
        String tmpFilePath = getTempStorageDirectory(false) + "destPath";
        if (storedEntityMetadata.getIePid() == null) {
            return null;
        }
        String existsDescPath = StorageUtil.readDestPathFromTmpFile(storedEntityMetadata.getIePid(), tmpFilePath, storedEntityMetadata.getEntityPid());
        log.debug("SLUBStoragePlugin.getFilePathInDescIfExists() existsDescPath='" + existsDescPath + "'");
        return existsDescPath;
    }

    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private boolean canHandleSourcePath(String srcPath) {
        log.info("SLUBStoragePlugin.canHandleSourcePath path=" + srcPath);
        try {
            File file = new File(srcPath);
            return file.canRead();
        }
        catch (Exception e) {
            log.warn("SLUBStoragePlugin.canHandleSourcePath exception=" + e.getMessage());
            return false;
        }
    }

    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private int getChecksummerAlgorithmIndex(String algorithm)
    {
        log.info("SLUBStoragePlugin.getChecksummerAlgorithm() algorithm='" + algorithm + "'");
        try
        {
            Fixity.FixityAlgorithm fixityAlgorithm = Fixity.FixityAlgorithm.valueOf(algorithm);
            return fixityAlgorithm.ordinal();
        }
        catch (Exception e) {
            log.info("SLUBStoragePlugin.getChecksummerAlgorithm(), exception: " + e.getMessage());
        }
        return -1;
    }




    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private File getCanonicalFile(String srcPath)
    {
        log.info("SLUBStoragePlugin.getCanonicalFile() srcPath='"+ srcPath + "'");
        String fileName = srcPath.split("\\" + File.separator)[(srcPath.split("\\" + File.separator).length - 1)];
        try
        {
            File canonicalSrcDir = new File(srcPath).getParentFile().getCanonicalFile();
            return new File(canonicalSrcDir, fileName).getCanonicalFile();
        }
        catch (IOException e) {
            log.warn("getCanonicalFile of '" + srcPath + "':" + e.getMessage());
        }
        return null;
    }

    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private Map<String, String> getStoreEntityIdentifier(StoredEntityMetaData storedEntityMetadata, String destFilePath) throws Exception
    {
        log.info("SLUBStoragePlugin.getStoreEntityIdentifier() destFilePath='" + destFilePath +"'");
        Map<String, String> paths = new HashMap<String, String>();
        log.debug("(1) storedEntityMetadata is null?" + (null == storedEntityMetadata));
        String fileName = createFileName(storedEntityMetadata);
        log.debug("fileName='" + fileName + "'");
        log.debug("(2) storedEntityMetadata is null?" + (null == storedEntityMetadata));
        String relativeDirectoryPath = getStreamRelativePath(storedEntityMetadata);
        log.debug("relativeDirectoryPath='" + relativeDirectoryPath + "'");
        if (destFilePath == null)
        {
            File destFile = getStreamDirectory(relativeDirectoryPath, fileName);
            destFilePath = destFile.getAbsolutePath();
        }
        log.debug("destFilePath (2)='" + destFilePath + "'");
        paths.put("destFilePath", destFilePath);
        // paths.put("relativeDirectoryPath", (relativeDirectoryPath + getNextDir(destFilePath) + File.separator + fileName));
        paths.put("relativeDirectoryPath", (relativeDirectoryPath + File.separator + fileName));
        return paths;

    }
    /** copied from NFS Storage Plugin, enhanced with debugging info,
     * this combines full file path and creates parent directories if needed  {@inheritDoc}
     */
    private File getStreamDirectory(String path, String fileName) {
        log.info("SLUBStoragePlugin.getStreamDirectory path='" + path + "' fileName='" + fileName + "'");
        File newDir = new File(parameters.get(DIR_ROOT) + File.separator + path);
        log.debug("SLUBStoragePlugin.getStreamDirectory newDir.getAbsolutePath()=" + newDir.getAbsolutePath());
        boolean arecreated = newDir.mkdirs();
        log.debug("SLUBStoragePlugin.getStreamDirectory newDir.mkdirs(), directories are created:" + arecreated);
        return new File(newDir.getAbsolutePath() + File.separator + fileName);
    }
    /** prepare right path
     * path should be of form yyyy/MM/dd/IE-PID/
     * we need to findout the associated dnx document (IE),
     * get the creation date of the IE/SIP
     * and get the IE PID of the associated IE.
     * @param storedEntityMetaData StoredEntityMetaData
     * @return calculated relative path as String
     * returns the path as string
     */
    private String getStreamRelativePath(StoredEntityMetaData storedEntityMetaData ) throws Exception {
        log.info("SLUBStoragePlugin.getStreamRelativePath()");
        if ((null == storedEntityMetaData)) throw new AssertionError();
        log.debug("SLUBStoragePlugin.getStreamRelativePath() assert fine");
        String relativeDirectoryPath = File.separator;
        // get IE PID by calling IE-DNX record and search for ""internalIdentifierType" == "PID"
        DnxDocument iedoc = storedEntityMetaData.getIeDnx();
        log.debug("SLUBStoragePlugin.getStreamRelativePath() getIeDnx fine");
        if (null == iedoc) {
            log.error ("SLUBStoragePlugin.getStreamRelativePath no iedoc found, do you use plugin for others than permanent data (IE and filestream)? You should not! Also do not use it for SIPstoragePermanentGroup!");
            throw new Exception("error, no iedoc found, do you use plugin for others than permanent data (IE and filestream)? You should not! Also do not use it for SIPstoragePermanentGroup!");
        }
        StoredEntityMetaData.EntityType entityType = storedEntityMetaData.getEntityType();
        log.debug("SLUBStoragePlugin.getStreamRelativePath() getEntityType fine");
        String entitytype = entityType.name();
        log.debug("entitytype='" + entitytype + "'");
        DnxSection iesec = iedoc.getSectionById("internalIdentifier");
        if (null == iesec) {
            log.error ("SLUBStoragePlugin.getStreamRelativePath no section in entity of type "+entitytype +" with 'internalIdentfier' in associated iedoc found, do you use plugin for others than permanent data (IE and filestream)? You should not! Also do not use it for SIPstoragePermanentGroup!");
            throw new Exception("error, no section in entity of type "+entitytype +" with 'internalIdentfier' in associated iedoc found, do you use plugin for others than permanent data (IE and filestream)? You should not! Also do not use it for SIPstoragePermanentGroup!");
        }
        String iepid = null;
        log.debug("SLUBStoragePlugin.getStreamRelativePath iesec=" + iesec.toString());
        List<DnxSectionRecord> records = iesec.getRecordList();
        for (DnxSectionRecord element : records) {
            if (element.getKeyById("internalIdentifierType").getValue().equals("PID")) {
                iepid = element.getKeyById("internalIdentifierValue").getValue(); // found IEPID
                break;
            }
        }
        // raise Exception if IEPID is null
        if (null == iepid) {
            log.error ("SLUBStoragePlugin.getStreamRelativePath iesec="+iesec.toString() );
            throw new Exception("error, could not get IEPID for storedEntityMetaData:"+storedEntityMetaData.toString() +" of type " + entitytype);
        }
        log.debug("SLUBStoragePlugin.getStreamRelativePath iepid=" + iepid + " (entitytype=" + entitytype + ")");
        // get creationDate of "objectCharacteristics"
        String datestring = iedoc.getSectionKeyValue("objectCharacteristics", "creationDate");
        Calendar date = Calendar.getInstance();
        // date ist there stored in format (example): 2014-01-15 14:28:01
        SimpleDateFormat sdf;
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setLenient(false); /* if parse errors, do not guess about */
        Date d = sdf.parse(datestring);
        date.setTime(d);
        log.debug("SLUBStoragePlugin.getStreamRelativePath creation Date read=" + datestring + " parsed=" + date.toString());
        // now create path in format /yyyy/MM/dd/IEPID/
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

    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private void hardLink(String srcPath, String destPath)
            throws IOException
    {
        log.info("SLUBStoragePlugin.hardLink srcPath='" + srcPath + "' destPath='" + destPath + "'");
        String command = "ln";
        ExecExternalProcess proc = new ExecExternalProcess();
        List<String> args = new LinkedList<String>();
        args.add(srcPath);
        args.add(destPath);
        int retValue = proc.execExternalProcess(command, args);
        if (retValue != 0) {
            throw new IOException("ln " + srcPath + " " + destPath + " failed " + proc.getErrorStream() + proc.getInputStream());
        }
    }
    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private void saveDestPathsTmpFile(String folder, String key, String path)
    {
        log.info("SLUBStoragePlugin.saveDestPathsTmpFile()");
        log.debug("SLUBStoragePlugin.saveDestPathsTmpFile folder='" + folder + "'");
        log.debug("SLUBStoragePlugin.saveDestPathsTmpFile key='" + key + "'");
        log.debug("SLUBStoragePlugin.saveDestPathsTmpFile path='" + path + "'");
        if (folder == null) {
            return;
        }
        String tmpFilePath = getTempStorageDirectory(false) + "destPath";
        log.debug("SLUBStoragePlugin.saveDestPathsTmpFile tmpFilePath='" + tmpFilePath + "'");
        File destPathDir = new File(getTempStorageDirectory(false) + "destPath" + File.separator);
        log.debug("SLUBStoragePlugin.saveDestPathsTmpFile destPathDir='" + destPathDir + "'");
        if (!destPathDir.exists()) {
            boolean res = destPathDir.mkdirs();
            if (!res) {
                log.error("SLUBStoragePlugin.saveDestPathsTmpFile() destPathdir='" + destPathDir + "' could not be created");
            }
        }
        StorageUtil.saveDestPathToTmpFile(folder, tmpFilePath, key, path);
    }

    /** copied from NFS Storage Plugin, enhanced with debugging info, {@inheritDoc} */
    private void softLink(String srcPath, String destPath)
            throws IOException
    {
        log.info("SLUBStoragePlugin.softLink srcPath='" + srcPath + "' destPath='" + destPath + "'");
        File source = new File(srcPath);
        if (!source.exists()) {
            throw new IOException("File " + source + " does not exist");
        }
        String command = "ln";
        ExecExternalProcess proc = new ExecExternalProcess();
        List<String> args = new ArrayList<String>();
        args.add("-s");
        args.add(srcPath);
        args.add(destPath);
        int retValue = proc.execExternalProcess(command, args);
        if (retValue != 0) {
            throw new IOException("ln -s " + srcPath + " " + destPath + " failed " + proc.getErrorStream() + proc.getInputStream());
        }
    }
}

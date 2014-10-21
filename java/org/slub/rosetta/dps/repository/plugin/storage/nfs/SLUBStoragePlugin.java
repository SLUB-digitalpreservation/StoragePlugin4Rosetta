/*

2014 by Andreas Romeyke (SLUB Dresden)

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
  private static final String DIR_ROOT = "DIR_ROOT"; /** {@inheritDoc} */
  private static final ExLogger log = ExLogger.getExLogger(SLUBStoragePlugin.class);

  public SLUBStoragePlugin() {
    log.info("SLUBStoragePlugin instantiated");
  }

    public byte[] retrieveEntityByRange(String storedEntityIdentifier, long start, long end)
            throws Exception
    {
        byte[] bytes = new byte[(int)(end - start + 1L)];
        RandomAccessFile file = null;
        try
        {
            file = new RandomAccessFile((String)this.parameters.get("DIR_ROOT") + storedEntityIdentifier, "r");
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

    public String getLocalFilePath(String storedEntityIdentifier)
    {
        return getFullFilePath(storedEntityIdentifier);
    }
    public String getFullFilePath(String storedEntityIdentifier)
    {
        return (String)this.parameters.get("DIR_ROOT") + storedEntityIdentifier;
    }
    public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier)
            throws Exception
    {
        return checkFixity(fixities, storedEntityIdentifier, true);
    }
    public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier, boolean isRelativePath)
            throws Exception
    {
        boolean result = true;
        if (fixities != null)
        {
            boolean calcMD5 = false;
            boolean calcSHA1 = false;
            boolean calcCRC32 = false;
            for (Fixity fixity : fixities)
            {
                fixity.setResult(null);
                if (Fixity.FixityAlgorithm.MD5.toString().equals(fixity.getAlgorithm()))
                {
                    calcMD5 = true;
                }
                else if (Fixity.FixityAlgorithm.SHA1.toString().equals(fixity.getAlgorithm()))
                {
                    calcSHA1 = true;
                }
                else if (Fixity.FixityAlgorithm.CRC32.toString().equals(fixity.getAlgorithm()))
                {
                    calcCRC32 = true;
                }
                else
                {
                    String oldValue = fixity.getValue();
                    fixity.setValue(getChecksumUsingPlugin(isRelativePath ? getLocalFilePath(storedEntityIdentifier) : storedEntityIdentifier, fixity.getPluginName(), oldValue));
                    fixity.setResult(Boolean.valueOf((oldValue == null) || (oldValue.equals(fixity.getValue()))));
                    result &= fixity.getResult().booleanValue();
                }
            }
            if ((calcMD5) || (calcSHA1) || (calcCRC32))
            {
                InputStream is = null;
                try
                {
                    is = retrieveEntity(storedEntityIdentifier, isRelativePath);
                    Checksummer checksummer = new Checksummer(is, calcMD5, calcSHA1, calcCRC32);
                    for (Fixity fixity : fixities)
                    {
                        int checksummerAlgorithmIndex = getChecksummerAlgorithmIndex(fixity.getAlgorithm());
                        if (checksummerAlgorithmIndex != -1)
                        {
                            String oldValue = fixity.getValue();
                            fixity.setValue(checksummer.getChecksum(fixity.getAlgorithm()));
                            fixity.setResult(Boolean.valueOf((oldValue == null) || (oldValue.equalsIgnoreCase(fixity.getValue()))));
                            result &= fixity.getResult().booleanValue();
                        }
                    }
                }
                finally
                {
                    Checksummer checksummer;
                    if (is != null) {
                        is.close();
                    }
                }
            }
        }
        return result;
    }
    private int getChecksummerAlgorithmIndex(String algorithm)
    {
        try
        {
            Fixity.FixityAlgorithm fixityAlgorithm = Fixity.FixityAlgorithm.valueOf(algorithm);
            return fixityAlgorithm.ordinal();
        }
        catch (Exception e) {}
        return -1;
    }

    public boolean deleteEntity(String storedEntityIdentifier)
    {
        File file = new File((String)this.parameters.get("DIR_ROOT") + storedEntityIdentifier);
        try
        {
            return file.delete();
        }
        catch (Exception e)
        {
            log.warn("failed to delete entity with path: " + file.getPath(), e.getMessage());
        }
        return true;
    }
    public InputStream retrieveEntity(String storedEntityIdentifier)
            throws IOException
    {
        return retrieveEntity(storedEntityIdentifier, true);
    }
    public InputStream retrieveEntity(String storedEntityIdentifier, boolean isRelative)
            throws IOException
    {
        return new FileInputStream((isRelative ? (String)this.parameters.get("DIR_ROOT") : "") + storedEntityIdentifier);
    }
    public String storeEntity(InputStream is, StoredEntityMetaData storedEntityMetadata)
            throws Exception
    {
        String existsDescPath = getFilePathInDescIfExists(storedEntityMetadata);

        String destFilePath = null;

        boolean isCopyFileNeeded = true;
        if (existsDescPath != null)
        {
            destFilePath = existsDescPath;
            isCopyFileNeeded = !checkFixity(storedEntityMetadata.getFixities(), destFilePath, false);
        }
        Map<String, String> paths = getStoreEntityIdentifier(storedEntityMetadata, destFilePath);
        String storedEntityIdentifier = (String)paths.get("relativeDirectoryPath");
        destFilePath = (String)paths.get("destFilePath");
        if (isCopyFileNeeded)
        {
            if (canHandleSourcePath(storedEntityMetadata.getCurrentFilePath()))
            {
                if (is != null) {
                    is.close();
                }
                copyStream(storedEntityMetadata, destFilePath);
            }
            else
            {
                log.info("Cannot handle source path: " + storedEntityMetadata.getCurrentFilePath());
                if (is == null)
                {
                    log.warn("InputStream is null");
                    return null;
                }
                FileOutputStream output = null;
                try
                {
                    output = new FileOutputStream(new File(destFilePath));
                    IOUtil.copy(is, output);
                }
                finally
                {
                    IOUtil.closeQuietly(output);
                }
            }
            if (!checkFixity(storedEntityMetadata.getFixities(), storedEntityIdentifier)) {
                return null;
            }
        }
        return storedEntityIdentifier;
    }
    protected void copyStream(StoredEntityMetaData storedEntityMetadata, String destPath)
            throws IOException
    {
        String filesHandlingMethod = (String)this.parameters.get("FILES_HANDLING_METHOD");
        String srcPath = storedEntityMetadata.getCurrentFilePath();

        String pid = storedEntityMetadata.getEntityPid();
        String iePid = storedEntityMetadata.getIePid();
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
    private void saveDestPathsTmpFile(String folder, String key, String path)
    {
        if (folder == null) {
            return;
        }
        String tmpFilePath = getTempStorageDirectory(false) + "destPath";


        File destPathDir = new File(getTempStorageDirectory(false) + "destPath" + File.separator);
        if (!destPathDir.exists()) {
            destPathDir.mkdirs();
        }
        StorageUtil.saveDestPathToTmpFile(folder, tmpFilePath, key, path);
    }
    private File getCanonicalFile(String srcPath)
    {
        String fileName = srcPath.split("\\" + File.separator)[(srcPath.split("\\" + File.separator).length - 1)];
        File canonicalSrcDir = null;
        try
        {
            canonicalSrcDir = new File(srcPath).getParentFile().getCanonicalFile();
            return new File(canonicalSrcDir, fileName).getCanonicalFile();
        }
        catch (IOException e) {
            log.warn("getCanonicalFile of '" + srcPath + "':" + e.getMessage());
        }
        return null;
    }

    private void hardLink(String srcPath, String destPath)
            throws IOException
    {
        String command = "ln";
        ExecExternalProcess proc = new ExecExternalProcess();
        List<String> args = new LinkedList();
        args.add(srcPath);
        args.add(destPath);
        int retValue = proc.execExternalProcess(command, args);
        if (retValue != 0) {
            throw new IOException("ln " + srcPath + " " + destPath + " failed " + proc.getErrorStream() + proc.getInputStream());
        }
    }

    private void softLink(String srcPath, String destPath)
            throws IOException
    {
        File source = new File(srcPath);
        if (!source.exists()) {
            throw new IOException("File " + source + " does not exist");
        }
        String command = "ln";
        ExecExternalProcess proc = new ExecExternalProcess();
        List<String> args = new ArrayList();
        args.add("-s");
        args.add(srcPath);
        args.add(destPath);
        int retValue = proc.execExternalProcess(command, args);
        if (retValue != 0) {
            throw new IOException("ln -s " + srcPath + " " + destPath + " failed " + proc.getErrorStream() + proc.getInputStream());
        }
    }

    protected String getFilePathInDescIfExists(StoredEntityMetaData storedEntityMetadata)
    {
        String tmpFilePath = getTempStorageDirectory(false) + "destPath";
        if (storedEntityMetadata.getIePid() == null) {
            return null;
        }
        String existsDescPath = StorageUtil.readDestPathFromTmpFile(storedEntityMetadata.getIePid(), tmpFilePath, storedEntityMetadata.getEntityPid());
        return existsDescPath;
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
    log.info("SLUBStoragePlugin.getStreamRelativePath iepid=" + iepid + " (entitytype="+ entitytype +")");
    // get creationDate of "objectCharacteristics"
    String datestring = iedoc.getSectionKeyValue("objectCharacteristics", "creationDate");
    Calendar date = Calendar.getInstance();
    // date ist there stored in format (example): 2014-01-15 14:28:01
    SimpleDateFormat sdf;
      sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      sdf.setLenient(false); /* if parse errors, do not guess about */
    Date d = sdf.parse(datestring);
    date.setTime(d);
    log.info("SLUBStoragePlugin.getStreamRelativePath creation Date read=" + datestring + " parsed=" + date.toString());
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

  /** copied from NFS Storage Plugin, enhanced with debugging info,
   * this combines full file path and creates parent directories if needed  {@inheritDoc} 
   */
  private File getStreamDirectory(String path, String fileName) {
    File newDir = new File(parameters.get(DIR_ROOT) + File.separator + path);
    log.info("SLUBStoragePlugin.getStreamDirectory path=" + path);
    log.info("SLUBStoragePlugin.getStreamDirectory fileName=" + fileName);
    log.info("SLUBStoragePlugin.getStreamDirectory newDir.getAbsolutePath()=" + newDir.getAbsolutePath());
    boolean arecreated = newDir.mkdirs();
    log.info("SLUBStoragePlugin.getStreamDirectory newDir.mkdirs(), directories are created:" + arecreated);
    return new File(newDir.getAbsolutePath() + File.separator + fileName);
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
private String getNextDir(String fullPath)
  {
    String[] dirs = fullPath.split("\\" + File.separator);
    return dirs[(dirs.length - 2)];
  }

private Map<String, String> getStoreEntityIdentifier(StoredEntityMetaData storedEntityMetadata, String destFilePath) throws Exception
  {
    Map<String, String> paths = new HashMap<String, String>();

    String fileName = createFileName(storedEntityMetadata);

    String relativeDirectoryPath = getStreamRelativePath(storedEntityMetadata);
    if (destFilePath == null)
    {
      File destFile = getStreamDirectory(relativeDirectoryPath, fileName);
      destFilePath = destFile.getAbsolutePath();
    }
    paths.put("destFilePath", destFilePath);
    paths.put("relativeDirectoryPath", (relativeDirectoryPath + getNextDir(destFilePath) + File.separator + fileName));
    return paths;

  }

}

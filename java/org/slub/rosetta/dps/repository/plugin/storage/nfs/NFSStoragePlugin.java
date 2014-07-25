// decompiled for interoperability via java decompiler
package com.exlibris.dps.repository.plugin.storage.nfs;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.infra.svc.api.scriptRunner.ExecExternalProcess;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.core.sdk.storage.handler.AbstractStorageHandler;
import com.exlibris.core.sdk.storage.handler.StorageUtil;
import com.exlibris.core.sdk.utils.FileUtil;
import com.exlibris.digitool.common.storage.Fixity;
import com.exlibris.digitool.common.storage.Fixity.FixityAlgorithm;
import com.exlibris.digitool.infrastructure.utils.Checksummer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class NFSStoragePlugin
extends AbstractStorageHandler
{
  private static final String DIR_PREFIX = "DIR_PREFIX";
  private static final String FILE_PER_DIR = "FILE_PER_DIR";
  private static final String DIR_ROOT = "DIR_ROOT";
  private static final String FILES_HANDLING_METHOD = "FILES_HANDLING_METHOD";
  private static final ExLogger log = ExLogger.getExLogger(NFSStoragePlugin.class);
  private final String RELATIVE_DIRECTORY_PATH = "relativeDirectoryPath";
  private final String DEST_FILE_PATH = "destFilePath";

  public boolean deleteEntity(String storedEntityIdentifier)
  {
    File file = new File((String)this.parameters.get("DIR_ROOT") + storedEntityIdentifier);
    try
    {
      return file.delete();
    }
    catch (Exception e)
    {
      log.warn("failed to delete entity with path: " + file.getPath(), new String[0]);
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
            log.warn("InputStream is null", new String[0]);
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
            checksummer = new Checksummer(is, calcMD5, calcSHA1, calcCRC32);
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

  private String getStreamRelativePath(String destFilePath)
  {
    StringBuffer relativeDirectoryPath = new StringBuffer();

    String year = null;String month = null;String day = null;
    if (destFilePath == null)
    {
      Date date = new Date();
      year = new SimpleDateFormat("yyyy").format(date);
      month = new SimpleDateFormat("MM").format(date);
      day = new SimpleDateFormat("dd").format(date);
    }
    else
    {
      String nextDir = getNextDir(destFilePath);
      String[] splitted = destFilePath.split(nextDir);
      splitted = StringUtils.split(splitted[0], File.separator);
      day = splitted[(splitted.length - 1)];
      month = splitted[(splitted.length - 2)];
      year = splitted[(splitted.length - 3)];
    }
    relativeDirectoryPath.append(File.separator).append(year).append(File.separator).append(month).append(File.separator).append(day).append(File.separator);






    return relativeDirectoryPath.toString();
  }

  private File getStreamDirectory(String path, String fileName)
  {
    String directoryPrefix = "fileset_";
    int maxFilesPerDir = 100;
    if (!StringUtils.isEmpty((String)this.parameters.get("DIR_PREFIX"))) {
      directoryPrefix = (String)this.parameters.get("DIR_PREFIX");
    }
    if (!StringUtils.isEmpty((String)this.parameters.get("FILE_PER_DIR"))) {
      maxFilesPerDir = Integer.valueOf((String)this.parameters.get("FILE_PER_DIR")).intValue();
    }
    File newDir = new File((String)this.parameters.get("DIR_ROOT") + File.separator + path);
    newDir.mkdirs();
    File destDir = FileUtil.getNextDirectory(newDir, directoryPrefix, maxFilesPerDir);

    return new File(destDir.getAbsolutePath() + File.separator + fileName);
  }

  private String getNextDir(String fullPath)
  {
    String[] dirs = fullPath.split("\\" + File.separator);
    return dirs[(dirs.length - 2)];
  }

  private boolean canHandleSourcePath(String srcPath)
  {
    try
    {
      File file = new File(srcPath);
      return file.canRead();
    }
    catch (Exception e) {}
    return false;
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

  protected String getFilePathInDescIfExists(StoredEntityMetaData storedEntityMetadata)
  {
    String tmpFilePath = getTempStorageDirectory(false) + "destPath";
    if (storedEntityMetadata.getIePid() == null) {
      return null;
    }
    String existsDescPath = StorageUtil.readDestPathFromTmpFile(storedEntityMetadata.getIePid(), tmpFilePath, storedEntityMetadata.getEntityPid());
    return existsDescPath;
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
    catch (IOException e) {}
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

  public String getFullFilePath(String storedEntityIdentifier)
  {
    return (String)this.parameters.get("DIR_ROOT") + storedEntityIdentifier;
  }

  public String getLocalFilePath(String storedEntityIdentifier)
  {
    return getFullFilePath(storedEntityIdentifier);
  }

  public boolean isAvailable()
  {
    try
    {
      File file = new File((String)this.parameters.get("DIR_ROOT"));
      if ((!file.exists()) &&
          (!file.mkdirs()))
      {
        log.error("No access to folder" + (String)this.parameters.get("DIR_ROOT"), new String[0]);
        return false;
      }
      if (!file.canRead())
      {
        log.error("No read access to folder: " + (String)this.parameters.get("DIR_ROOT"), new String[0]);
        return false;
      }
      if (!file.canWrite())
      {
        log.error("No write access to folder: " + (String)this.parameters.get("DIR_ROOT"), new String[0]);
        return false;
      }
    }
    catch (Exception e)
    {
      log.error("isAvailable method fell for storage: " + getStorageId(), e, new String[0]);
      return false;
    }
    return true;
  }

  public byte[] retrieveEntityByRange(String storedEntityIdentifier, long start, long end)
    throws Exception
    {
      bytes = new byte[(int)(end - start + 1L)];
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
            log.warn("Failed closing file", new String[0]);
          }
        }
      }
    }

  private Map<String, String> getStoreEntityIdentifier(StoredEntityMetaData storedEntityMetadata, String destFilePath)
  {
    Map<String, String> paths = new HashMap();

    String fileName = createFileName(storedEntityMetadata);
    String relativeDirectoryPath = getStreamRelativePath(destFilePath);
    if (destFilePath == null)
    {
      File destFile = getStreamDirectory(relativeDirectoryPath, fileName);
      destFilePath = destFile.getAbsolutePath();
    }
    paths.put("destFilePath", destFilePath);
    paths.put("relativeDirectoryPath", relativeDirectoryPath + getNextDir(destFilePath) + File.separator + fileName);
    return paths;
  }
}

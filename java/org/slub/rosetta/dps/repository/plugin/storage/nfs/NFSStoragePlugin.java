// decompiled for interoperability via java decompiler
package com.exlibris.dps.repository.plugin.storage.nfs;

import com.exlibris.core.infra.common.exceptions.logging.ExLogger;
import com.exlibris.core.infra.common.util.IOUtil;
import com.exlibris.core.infra.svc.api.scriptRunner.ExecExternalProcess;
import com.exlibris.core.sdk.storage.containers.StoredEntityMetaData;
import com.exlibris.core.sdk.storage.handler.AbstractStorageHandler;
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
  
  public boolean deleteEntity(String storedEntityIdentifier)
  {
    File file = new File((String)parameters.get("DIR_ROOT") + storedEntityIdentifier);
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
    return new FileInputStream((String)parameters.get("DIR_ROOT") + storedEntityIdentifier);
  }
  
  public String storeEntity(InputStream is, StoredEntityMetaData storedEntityMetadata)
    throws Exception
  {
    String fileName = createFileName(storedEntityMetadata);
    
    String relativeDirectoryPath = getStreamRelativePath();
    File destFile = getStreamDirectory(relativeDirectoryPath, fileName);
    if (canHandleSourcePath(storedEntityMetadata.getCurrentFilePath()))
    {
      is.close();
      copyStream(storedEntityMetadata.getCurrentFilePath(), destFile.getAbsolutePath());
    }
    else
    {
      IOUtil.copy(is, new FileOutputStream(destFile));
    }
    String storedEntityIdentifier = relativeDirectoryPath + getNextDir(destFile.getAbsolutePath()) + File.separator + fileName;
    if (!checkFixity(storedEntityMetadata.getFixities(), storedEntityIdentifier))
    {
      deleteEntity(storedEntityIdentifier);
      return null;
    }
    return storedEntityIdentifier;
  }
  
  public boolean checkFixity(List<Fixity> fixities, String storedEntityIdentifier)
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
          fixity.setValue(getChecksumUsingPlugin(getLocalFilePath(storedEntityIdentifier), fixity.getPluginName(), oldValue));
          fixity.setResult(Boolean.valueOf((oldValue == null) || (oldValue.equals(fixity.getValue()))));
          result &= fixity.getResult().booleanValue();
        }
      }
      if ((calcMD5) || (calcSHA1) || (calcCRC32))
      {
        InputStream is = null;
        try
        {
          is = retrieveEntity(storedEntityIdentifier);
          checksummer = new Checksummer(is, calcMD5, calcSHA1, calcCRC32);
          for (Fixity fixity : fixities)
          {
            int checksummerAlgorithmIndex = getChecksummerAlgorithmIndex(fixity.getAlgorithm());
            if (checksummerAlgorithmIndex != -1)
            {
              String oldValue = fixity.getValue();
              fixity.setValue(checksummer.getChecksum(fixity.getAlgorithm()));
              fixity.setResult(Boolean.valueOf((oldValue == null) || (oldValue.equals(fixity.getValue()))));
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
  
  private String getStreamRelativePath()
  {
    String relativeDirectoryPath = "";
    Date date = new Date();
    
    relativeDirectoryPath = relativeDirectoryPath + File.separator;
    relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("yyyy").format(date);
    relativeDirectoryPath = relativeDirectoryPath + File.separator;
    relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("MM").format(date);
    relativeDirectoryPath = relativeDirectoryPath + File.separator;
    relativeDirectoryPath = relativeDirectoryPath + new SimpleDateFormat("dd").format(date);
    relativeDirectoryPath = relativeDirectoryPath + File.separator;
    
    return relativeDirectoryPath;
  }
  
  private File getStreamDirectory(String path, String fileName)
  {
    String directoryPrefix = "fileset_";
    int maxFilesPerDir = 100;
    if (!StringUtils.isEmpty((String)parameters.get("DIR_PREFIX"))) {
      directoryPrefix = (String)parameters.get("DIR_PREFIX");
    }
    if (!StringUtils.isEmpty((String)parameters.get("FILE_PER_DIR"))) {
      maxFilesPerDir = Integer.valueOf((String)parameters.get("FILE_PER_DIR")).intValue();
    }
    File newDir = new File((String)parameters.get("DIR_ROOT") + File.separator + path);
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
  
  protected void copyStream(String srcPath, String destPath)
    throws IOException
  {
    String filesHandlingMethod = (String)parameters.get("FILES_HANDLING_METHOD");
    if ("move".equalsIgnoreCase(filesHandlingMethod))
    {
      File canonicalSrcFile = getCanonicalFile(srcPath);
      FileUtil.moveFile(canonicalSrcFile, new File(destPath));
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
    }
  }
  
  private File getCanonicalFile(String srcPath)
    throws IOException
  {
    String fileName = srcPath.split("\\" + File.separator)[(srcPath.split("\\" + File.separator).length - 1)];
    File canonicalSrcDir = new File(srcPath).getParentFile().getCanonicalFile();
    File canonicalSrcFile = new File(canonicalSrcDir, fileName).getCanonicalFile();
    return canonicalSrcFile;
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
    return (String)parameters.get("DIR_ROOT") + storedEntityIdentifier;
  }
  
  public String getLocalFilePath(String storedEntityIdentifier)
  {
    return getFullFilePath(storedEntityIdentifier);
  }
  
  public boolean isAvailable()
  {
    try
    {
      File file = new File((String)parameters.get("DIR_ROOT"));
      if ((!file.exists()) && 
        (!file.mkdirs()))
      {
        log.error("No access to folder" + (String)parameters.get("DIR_ROOT"), new String[0]);
        return false;
      }
      if (!file.canRead())
      {
        log.error("No read access to folder:  " + (String)parameters.get("DIR_ROOT"), new String[0]);
        return false;
      }
      if (!file.canWrite())
      {
        log.error("No write access to folder:  " + (String)parameters.get("DIR_ROOT"), new String[0]);
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
      file = new RandomAccessFile((String)parameters.get("DIR_ROOT") + storedEntityIdentifier, "r");
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
}


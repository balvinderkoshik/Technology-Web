package com.express.util

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation


/**
  * Utility to rename files in the specified folder.
  * The utility accepts the folder location and the suffix that is to be added to the files.
  *
  * @author mbadgujar
  */
object HDFSRenameUtil extends LazyLogging {

  private val config = new Configuration
  private val HDP_HADOOP_HOME = "/etc/hadoop/conf"

  config.addResource(new Path(HDP_HADOOP_HOME, "hdfs-site.xml"))
  config.addResource(new Path(HDP_HADOOP_HOME, "core-site.xml"))
  config.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
  config.set("fs.file.impl", classOf[LocalFileSystem].getName)
  config.set("hadoop.security.authentication", "kerberos")


  /**
    * Rename all the files in provided folder. Does not recurse in to folders with the folder.
    *
    * @param folder     HDFS Folder
    * @param fileSuffix File suffix to be appended
    *
    */
  private def rename(folder: String, fileSuffix: String): Unit = {
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    val locatedFileStatusRemoteIterator = dfs.listFiles(new Path(folder), false)
    while (locatedFileStatusRemoteIterator.hasNext) {
      val file = locatedFileStatusRemoteIterator.next.getPath.getName
      val renamedFile = {
        if (file.contains(fileSuffix))
          file.replaceAll("_" + fileSuffix, "") + "_" + fileSuffix
        else
          file + "_" + fileSuffix
      }
      logger.info("Renaming file:" + file + " to:" + renamedFile)
      val status = dfs.rename(new Path(folder + "/" + file), new Path(folder + "/" + renamedFile))
      if (!status) logger.error("Renaming file: " + file + " failed")
    }
    logger.info("Renaming file(s) from folder: " + folder + " completed successfully")
    dfs.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) throw new Exception("Please provide following options in order: " + "1. HDFS folder Path " + "2. File-Suffix")
    val folder = args(0)
    val suffix = args(1)
    rename(folder, suffix)
  }

}

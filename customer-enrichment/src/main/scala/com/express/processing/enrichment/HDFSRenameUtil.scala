package com.express.processing.enrichment


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.FileUtil._
import java.io._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * Utility to rename files in the specified folder.
  * The utility accepts the folder location and the suffix that is to be added to the files.
  *
  * @author mbadgujar
  */
object HDFSRenameUtil  {

  @transient
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
    * @param fileList   HDFS Folder
    * @param fileSuffix File suffix to be appended
    *
    */
  def rename(fileList: Array[String], fileSuffix: String): Unit = {
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    fileList.map(file =>
       {
        val newFile = file.replaceAllLiterally(fileSuffix,"")
        dfs.rename(new Path(file), new Path(s"$newFile$fileSuffix"))
      }
    )
    dfs.close()
  }


  def copyHDFS(srcPath:String, targetPath: String): Boolean = {
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    copy(dfs,new Path(srcPath),dfs,new Path(targetPath),false,true,config)

  }

  def listFilesHDFS(folderPath : String,fileFilter: FilenameFilter,filterString: Option[String]): ArrayBuffer[String] ={
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    val fileListIterator = dfs.listFiles(new Path(folderPath), true)
    val fileList = mutable.ArrayBuffer[String]()
    while(fileListIterator.hasNext) {
      fileList+=fileListIterator.next().getPath.toString
    }
    filterString match {
      case Some(filterStr) => fileList.filter(a => fileFilter.accept(new File(a), filterStr))
      case None => fileList
    }
  }

  def moveHDFS(srcFolderPath : String, destFolderPath : String): Boolean ={
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    dfs.rename(new Path(srcFolderPath),new Path(destFolderPath))

  }

  def makeDirHDFS(dirPath:String): Boolean = {
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    dfs.mkdirs(new Path(dirPath))
  }

  def deleteHDFS(pathWithWindChar: String): Boolean = {
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    dfs.delete(new Path(pathWithWindChar),true)
  }

  def closeHDFS(): Unit ={
    UserGroupInformation.setConfiguration(config)
    UserGroupInformation.getLoginUser
    val dfs = FileSystem.get(config)
    dfs.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) throw new Exception("Please provide following options in order: " + "1. HDFS file List " + "2. File-Suffix")
    val files = Array(args(0))
    val suffix = args(1)
    rename(files, suffix)
  }

}

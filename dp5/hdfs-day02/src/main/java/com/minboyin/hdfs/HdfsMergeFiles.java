package com.minboyin.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by minbo on 2021/1/11 0:49
 */

public class HdfsMergeFiles {
    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        HdfsMergeFiles hdfsMergeFiles = new HdfsMergeFiles();
        //hdfsMergeFiles.fileDownload();
        hdfsMergeFiles.fileUpload();

    }

    /**
     * MergeFilesUpload
     */

    public void fileUpload() throws IOException, URISyntaxException, InterruptedException {
        //1、获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata01:8020"), new Configuration(), "root");

        //2、获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //3、获取分布式文件系统的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test1/test_big.txt"));

        //4、获取本地文件系统中文件状态：方法一：fileSystem.listStatus；方法二：fileSystem.listFiles
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("D://data"));

        //5、遍历本体文件系统状态，获取各个文件的输出流;方法一：流传输；方法二：copyFromLocalFile
        for (FileStatus fileStatus:fileStatuses){
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath()); //获取文件的输出流
            //6、将本地各个文件的文件流复制到大文件中
            IOUtils.copy(inputStream,outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        //7、关闭流
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
        localFileSystem.close();
    }

    /**
     * MergeFilesDownload
     */

    public void fileDownload() throws URISyntaxException, IOException, InterruptedException {
        //1、获取分布式文件系统：
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://bigdata01:8020"),new Configuration(),"root");

        //2、调用方法获取获取文件夹的所有文件的详情：方法一：fileSystem.listStatus；方法二：fileSystem.listFiles
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/test"));

        //3、获取本地文件系统
        LocalFileSystem localFileSystem=FileSystem.getLocal(new Configuration());

        //4、获取一个本地文件系统的大文件输出流
        FSDataOutputStream outputStream=localFileSystem.create(new Path("D://data/test_big.txt"));

        //5、遍历各个文件，获取hdfs系统中各个文件的输入流；方法一：流传输；方法二：copyToLocalFile
        for (FileStatus fileStatus:fileStatuses){
            FSDataInputStream inputStream=fileSystem.open(fileStatus.getPath()); // 获取文件输出流

            //6、将各个文件的文件流复制到大文件
            IOUtils.copy(inputStream,outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        //7、关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();

    }
    /**
     * regex：find specificFiles that match what you want
     */
    public void getSpecificFiles() throws URISyntaxException, IOException, InterruptedException {
        //获取分布式文件系统
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://bigdata01:8020"),new Configuration(),"root");

        //获取文件系统中文件状态
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        //获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        //获取文件的输出流
        FSDataOutputStream outputStream = localFileSystem.create(new Path("D://data/test_big.txt"));

        // mess:
        Path[] paths = FileUtil.stat2Paths(fileStatuses);

        for (Path path:paths){
            //获取带路径的文件名字符串，没有实际使用，直接使用path
            String fileNamePath=path.toString();
            //Path filePath = new Path(fileNamePath);

            //获取文件名
            String fileName=path.getName();

            //设置regex:20200101.txt~20200131.txt
            String regex="2020+01+[0-3][1-9]+.+txt";

            //mess
            Pattern pattern=Pattern.compile(regex);

            //mess
            Matcher matcher=pattern.matcher(fileName);

            if (matcher.matches()){
                //找到符合匹配的文件,获取文件的输出流
                FSDataInputStream inputStream = fileSystem.open(path);

                //将各个文件的文件流复制到大文件
                IOUtils.copy(inputStream,outputStream);
                IOUtils.closeQuietly(inputStream);
            }
        }
        //关闭流
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }
    /**
     * how to deal with exception ,in order to make your code more Robust
     */
    public void HdfsMergeFile() throws URISyntaxException, IOException, InterruptedException {
        try {
            //1、获取分布式文件系统：
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata01:8020"), new Configuration(), "root");

            //2、获取本地文件系统
            LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

            //3、获取一个本地文件系统的大文件输出流
            FSDataOutputStream outputStream = localFileSystem.create(new Path("D://data/test_big.txt"));

            //4、调用方法获取获取文件夹的所有文件的详情：方法一：fileSystem.listStatus；方法二：fileSystem.listFiles
            //exception:判断文件路径是否存在，将空文件夹与正常文件夹同样处理
            String filePath="/test";
            Path targetFilePath=new Path(filePath);
            if (!fileSystem.exists(targetFilePath)){
                System.out.println("hdfs文件路径:" + filePath + "不存在");
            }
            else {
                FileStatus[] fileStatuses = fileSystem.listStatus(targetFilePath);
                try {
                    //5、遍历各个文件，获取hdfs系统中各个文件的输入流；方法一：流传输；方法二：copyToLocalFile
                    for (FileStatus fileStatus :fileStatuses ) {
                        FSDataInputStream inputStream = fileSystem.open(fileStatus.getPath()); // 获取文件输出流

                        //6、将各个文件的文件流复制到大文件
                        IOUtils.copy(inputStream, outputStream);
                        IOUtils.closeQuietly(inputStream);
                    }
                } catch (IOException e) {
                    System.out.println("文件传输异常：" + e.getMessage());
                } finally {
                    //7、关闭流
                    IOUtils.closeQuietly(outputStream);
                    localFileSystem.close();
                    fileSystem.close();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package com.demo;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsUtils {
    /*Java 操作 hadoop 文件系统
     *  FileSystem  客户端类
     * */
    FileSystem fs = null;

    /*
     * 初始化 hadoop hdfs 配置信息
     * */
    @Before
    public void init() {
        try {
            URI uri = new URI("hdfs:192.168.75.129:9000");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.75.129:9000");
            conf.set("dfs.replication", "1");
            fs = FileSystem.get(uri, conf, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * 创建一个文件夹到 hdfs:192.168.75.129:9000
     * */
    @Test
    public void mkdir() {
        try {
            Path path = new Path("/IDEAtest/");
            fs.mkdirs(path);
            System.out.println("创建成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("创建失败");
        }
    }

    /*
     *上传文件到 hdfs:192.168.75.129:9000
     */
    @Test
    public void upload() {
        try {
            Path src = new Path("G:\\hadoop\\happy.txt");
            Path dst = new Path("/test/hapyy.rar");
            fs.copyFromLocalFile(src, dst);
            System.out.println("上传成功");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("上传失败");
        }
    }

    /*
     *从 hdfs:192.168.137.128:9000 hadoop分布式文件系统
     *   下载文件到本地
     * */
    @Test
    public void down() {
        try {
            Path src = new Path("/eclipse/shenxian.txt");
            Path dst = new Path("C:\\Users\\温鑫\\Desktop\\day1220.rar");
            fs.copyToLocalFile(false, src, dst, true);
            System.out.println("下载成功");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("下载失败");
        }
    }

    /*
     * hdfs:192.168.75.129:9000 hadoop分布式文件系统
     * 执行删除文件或者文件夹
     * */
    @Test
    public void delet() {
        try {
            Path src = new Path("/joker/");
            fs.delete(src, true);
            System.out.println("删除成功");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("删除失败");
        }
    }

    /*hdfs:192.168.75.129:9000 hadoop分布式文件系统
     * 遍历 文件 或者文件夹
     * */
    @Test
    public void list() {
        try {
            Path src = new Path("/IDEA/hadoop");
            RemoteIterator<LocatedFileStatus> list =
                    fs.listLocatedStatus(src);
            System.out.println("该路径下所有目录及文件遍历如下：");
            while (list.hasNext()) {
                LocatedFileStatus lf = list.next();
                System.out.println(lf.getPath().getName());
            }
        } catch (Exception e) {
            System.out.println("操作失败");
            e.printStackTrace();
        }
    }
}

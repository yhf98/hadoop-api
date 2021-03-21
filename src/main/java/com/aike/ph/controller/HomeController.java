package com.aike.ph.controller;

import org.apache.hadoop.fs.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;


@RestController
@RequestMapping("/home")
public class HomeController {
    @GetMapping("/index")
    public String index(){
        return "Hadoop测试接口";
    }

    /**
     * 新建文件夹
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    @GetMapping("/mkdir")
    public String mkdir(@RequestParam(value = "") String path) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        String newDir = "/home/"+path;
        System.out.println("创建路径："+newDir);
        boolean result = hdfs.mkdirs(new Path(newDir));
        if (result) {
            System.out.println("Success!");
            return "Success!";
        }else {
            System.out.println("Failed!");
            return "Failed!";
        }

    }

    /**
     * 新建文件
     * @param name
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    @GetMapping("/touchFile")
    public String touchFile(@RequestParam(value = "") String name) throws IOException, URISyntaxException {
        Configuration configuration = new Configuration();

        String hdfsPath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), configuration);

        String filePath = "/home/"+name;

        FSDataOutputStream create = hdfs.create(new Path(filePath));

        System.out.println("Finish!");

        return "新建文件："+name;
    }

    /**
     * 将本地操作系统上的文件/home
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    @GetMapping("/copyFromLocalFile")
    public String copyFromLocalFile(@RequestParam(value = "") String path) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        String from_Linux = "D:/Dev/hadoop/home/hadoop-2.10.1/etc/靓仔.txt";
        String to_HDFS = "/home/";
        hdfs.copyFromLocalFile(new Path(from_Linux), new Path(to_HDFS));
        System.out.println("Finish!");
        return "上传成功："+path;
    }

    /**
     * 将本地操作系统上的文件/home
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    @GetMapping("/down")
    public String down(@RequestParam(value = "") String path) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String hdfsPath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(new URI(hdfsPath), conf);
        String from_HDFS = "/home/靓仔.txt";
        String to_Linux = "D:\\Dev\\hadoop\\home\\hadoop-2.10.1\\etc";
        hdfs.copyToLocalFile(false, new Path(from_HDFS), new Path(to_Linux));
        System.out.println("Finish!");
        return "下载："+path;
    }
    /**
     * list
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    @GetMapping("/list")
    public String list(@RequestParam(value = "") String path) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String hdfspath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(URI.create(hdfspath), conf);
        String watchHDFS = "/"+path;

        iteratorListFile(hdfs, new Path(watchHDFS));

        return "YYY";
    }

    @GetMapping("/blockInfo")
    public String blockInfo(@RequestParam(value = "") String path) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        String hdfspath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(new URI(hdfspath), conf);

        Path file = new Path("/home"+path);
        FileStatus fileStatus = hdfs.getFileStatus(file);

        BlockLocation[] location = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : location) {
            String[] hosts = block.getHosts();
            for (String host : hosts) {
                System.out.println("block:" +block + " host:"+ host);
            }
        }
        return "/home/"+path;
    }

    @GetMapping("/write")
    public String write(@RequestParam(value = "") String path) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();

        String hdfsPath = "hdfs://hadoop.aikezc.com:8020";
        FileSystem hdfs = FileSystem.get(URI.create(hdfsPath), conf);

        String filePath = "/home/靓仔.txt";

        FSDataOutputStream create = hdfs.create(new Path(filePath));

        System.out.println("Step 1 Finish!");

        String sayHi = path;
        byte[] buff = sayHi.getBytes();
        create.write(buff, 0, buff.length);
        create.close();
        System.out.println("Step 2 Finish!");

        return path;
    }


    public static void iteratorListFile(FileSystem hdfs, Path path)
            throws FileNotFoundException, IOException {
        FileStatus[] files = hdfs.listStatus(path);
        for (FileStatus file : files) {
            if (file.isDirectory()) {
                System.out.println(file.getPermission() + " " + file.getOwner()
                        + " " + file.getGroup() + " " + file.getPath());
                iteratorListFile(hdfs, file.getPath());
            } else if (file.isFile()) {
                System.out.println(file.getPermission() + " " + file.getOwner()
                        + " " + file.getGroup() + " " + file.getPath());
            }
        }
    }

}

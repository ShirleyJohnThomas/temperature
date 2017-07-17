package com.blossom.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午2:10:07
 */
public class HdfsUtils {

	/**
	 * 
	 * @description 创建文件
	 * @author Dell030
	 * @time 2017年7月17日 下午2:17:09 void
	 * @param url
	 * @param path
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void createFile(String url, String path) throws URISyntaxException, IOException {
		// 该类的对象封装了客户端或者服务器的配置
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		// 该类的对象是一个文件系统对象，可以用该对象的一些方法类对文件进行操作
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		// 设置目标路径
		Path filePath = new Path(path);
		fileSystem.create(filePath, new Progressable() {
			@Override
			public void progress() {
				System.out.println(">>");
			}
		});
		System.out.println("create dir successful!!!");
	}

	/**
	 * @description 验证文件是否存在HDFS当中
	 * @author Dell030
	 * @time 2017年7月17日 下午2:17:50 boolean
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static boolean ifFileExits(String filePath) throws IOException {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(filePath), configuration);
		Path path = new Path(filePath);
		return fileSystem.exists(path);
	}

	/**
	 * @description 删除文件
	 * @author Dell030
	 * @time 2017年7月17日 下午2:21:00 void
	 * @param url
	 * @param filePath
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static boolean delete(String url, String filePath) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		Path path = new Path(filePath);
		return fileSystem.deleteOnExit(path);
	}

	/**
	 * @description 上传本地文件
	 * @author Dell030
	 * @time 2017年7月17日 下午2:25:40 boolean
	 * @param url
	 * @param src
	 *            本地路径
	 * @param dst
	 *            hdfs路径
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void uploadFile(String url, String src, String dst) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		Path srcPath = new Path(src); // 本地路径
		Path dstPath = new Path(dst); // hdfs路径
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		fileSystem.copyFromLocalFile(false, srcPath, dstPath);
		// 打印文件路径
		System.out.println("Upload to " + configuration.get("fs.default.name"));
		System.out.println("------------list files------------" + "\n");
		// 列出文件
		FileStatus[] fileStatus = fileSystem.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fileSystem.close();
	}

	/**
	 * @description 从HDFS当中下载文件
	 * @author Dell030
	 * @time 2017年7月17日 下午2:32:48 void
	 * @param url
	 * @param src
	 * @param dst
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void downFile(String url, String src, String dst) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		Path srcPath = new Path(src);
		Path dstPath = new Path(dst);
		// 调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
		fileSystem.copyToLocalFile(false, srcPath, dstPath);
		// 打印文件路径
		System.out.println("Upload to " + configuration.get("fs.default.name"));
		System.out.println("------------list files------------" + "\n");
		FileStatus[] fileStatus = fileSystem.listStatus(dstPath);
		for (FileStatus file : fileStatus) {
			System.out.println(file.getPath());
		}
		fileSystem.close();
	}

	/**
	 * @description 文件重命名
	 * @author Dell030
	 * @time 2017年7月17日 下午2:37:59 void
	 * @param url
	 * @param oldName
	 * @param newName
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static boolean rename(String url, String oldName, String newName) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		Path oldPath = new Path(oldName);
		Path newPath = new Path(newName);
		return fileSystem.rename(oldPath, newPath);
	}

	/**
	 * @description 创建目录
	 * @author Dell030
	 * @time 2017年7月17日 下午2:42:44 void
	 * @param url
	 * @param path
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void mkdir(String url, String path) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		Path dirPath = new Path(path);
		fileSystem.mkdirs(dirPath);
		fileSystem.close();
	}

	/**
	 * @description 读取文件的内容
	 * @author Dell030
	 * @time 2017年7月17日 下午2:51:43 void
	 * @param url
	 * @param filePath
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	public static void readFile(String url, String filePath) throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		URI uri = new URI(url);
		FileSystem fileSystem = FileSystem.get(uri, configuration);
		Path path = new Path(filePath);
		InputStream input = fileSystem.open(path);
		IOUtils.copyBytes(input, System.out, 4096, false);

		IOUtils.closeStream(input);
	}

	/**
	 * @description 显示Hdfs一组路径的文件信息
	 * @author Dell030
	 * @time 2017年7月17日 下午2:52:20 void
	 * @param args
	 * @throws Exception
	 */
	public static void ListStatus(String args[]) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path[] paths = new Path[args.length];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = new Path(args[i]);
		}
		FileStatus[] status = fs.listStatus(paths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p);
		}

	}

	/**
	 * @description 查找文件所在的数据块
	 * @author Dell030
	 * @time 2017年7月17日 下午2:53:19 void
	 * @param url
	 * @param filename
	 * @throws Exception
	 */
	public static void GetBlockInfo(String url, String filename) throws Exception {
		Configuration conf = new Configuration();
		URI uri = new URI(url);
		FileSystem fs = FileSystem.get(uri, conf);
		Path path = new Path(filename);
		FileStatus fileStatus = fs.getFileStatus(path);
		BlockLocation[] blkloc = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen()); // 查找文件所在数据块
		for (BlockLocation loc : blkloc) {
			for (int i = 0; i < loc.getHosts().length; i++) {
				System.out.println(loc.getHosts()[i]);
			}
		}
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		String url = "hdfs://192.168.52.140:9000";
		// CreateFile("hdfs://192.168.52.140:9000","hdfs://192.168.52.140:9000/dirs2");
		// delete("hdfs://192.168.52.140:9000","hdfs://192.168.52.140:9000/dirs");
		// rename(url,"hdfs://192.168.52.140:9000/dir","hdfs://192.168.52.140:9000/newdir");
		// mkdir(url,"hdfs://192.168.52.140:9000/mydir");
		String relativelyPath = System.getProperty("user.dir") + "/src/log4j.properties";
		String downPath = System.getProperty("user.dir") + "/";
		// uploadFile(url,relativelyPath,"hdfs://192.168.52.140:9000/mydir");

		// readFile(url,"hdfs://192.168.52.140:9000/mydir/log4j.properties");
		// downFile(url,"hdfs://192.168.52.140:9000/mydir/log4j.properties","D:/");
		// System.out.println(IfFileExits("hdfs://192.168.52.140:9000/mydir/log4j.properties"));

		GetBlockInfo(url, "hdfs://192.168.52.140:9000/mydir/log4j.properties");

	}

}

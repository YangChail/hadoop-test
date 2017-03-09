package org.conan.myhadoop.hdfs;

import static org.junit.Assert.*;

import java.io.FilterOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HDFSClient {

	FileSystem fs = null;

	@Before
	public void conf() throws IOException {
		
		Configuration con = new Configuration();
		fs = FileSystem.get(con);
		

	}

	@Test
	public void download() throws IOException {
		Path path = new Path("hdfs://master:9000/jdk-7u79-linux-x64.gz");
		Path path1 = new Path("/home/hadoop/test");
		fs.copyToLocalFile(path, path1);
		
	}
	
	
	@Test
	public void upload() throws IOException {
		Path path = new Path("/");
		Path path1 = new Path("/home/hadoop/test/jdk");
		fs.copyFromLocalFile(path1, path);
		
	}
	@Test
	public void ls() throws IOException{
		Path path = new Path("/");
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path, true);
		while(listFiles.hasNext()){
			LocatedFileStatus next = listFiles.next();
			String name = next.getPath().getName();
			System.out.println(name);
		
		}
	
		
		
	}
	
	
	

}

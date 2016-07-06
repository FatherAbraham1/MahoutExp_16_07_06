package fp;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.mahout.common.HadoopUtil;

/**
 * 解析序列化结果
 * @author root
 *
 */
public class Parser {
	private Parser(){}
	@SuppressWarnings("deprecation")
	public static boolean Parser(String input,String output,String jobtracker,AKVRegex regex)throws IOException{
		boolean flag=true;
		Configuration conf =new Configuration();
		conf.set("mapreduce.jobtracker.address",jobtracker);
		
		FileSystem fsIn=FileSystem.get(URI.create(input),conf);
		FileSystem fsOut=FileSystem.get(URI.create(output),conf);
//		HadoopUtil.delete(conf, new Path(output));
		Path pathIn=new Path(input);
		Path pathOut=new Path(output);
		SequenceFile.Reader reader =null;
		FSDataOutputStream out=fsOut.create(pathOut);
	
		try{
			reader =new SequenceFile.Reader(fsIn, pathIn,conf);
			Writable key=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value=(Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key,value)){
				String k=regex.keyRegex(key);
				System.out.println(k);
				String v=regex.valueRegex(value);
				System.out.println(v);
				if(" ".equals(v)||v==null){
					continue;
				}
				out.writeBytes(k+"\t"+v+"\n");
			}
		}catch(IOException e){
			flag=false;
		}finally{
			IOUtils.closeStream(reader);
			out.close();
		}
		return flag;
	}
}

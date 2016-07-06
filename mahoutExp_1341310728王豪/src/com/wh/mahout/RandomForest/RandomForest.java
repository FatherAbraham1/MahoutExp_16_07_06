package com.wh.mahout.RandomForest;


import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.mapreduce.BuildForest;
import org.apache.mahout.classifier.df.mapreduce.TestForest;
import org.apache.mahout.classifier.df.tools.Describe;
import org.apache.mahout.common.HadoopUtil;

public class RandomForest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void run() throws Exception{
		String strInPath = "hdfs://master:9000/usr/wh/input/RDFores/rfdata";
		String strOutPath="hdfs://master:9000/usr/wh/output/RDFores/1341310728";
		//String outPath="hdfs://master:9000/usr/wh/Result/RDFores/1341310728";
		String outPath="file:///home/hadoop/Result/RDFores/1341310728";
		Configuration conf = new Configuration();
		HadoopUtil.delete(conf,new Path(strOutPath));
		String buildPath = strOutPath+"-bulider";
		HadoopUtil.delete(conf, new Path(buildPath));
		HadoopUtil.delete(conf, new Path(outPath));
		String [] pa = {"-d","I","9","N","L",
				"-p",strInPath,
				"-f",strOutPath+".info"};
		HadoopUtil.delete(conf, new Path(pa[Arrays.asList(pa).indexOf("-f")+1]));
		Describe.main(pa);
		
		String [] arg2 = {"-d",strInPath,
							"-t","5",
							"-o",strOutPath+"-builder",
							"-sl","3",
							"-ds",strOutPath+".info",
							"-ms","3"};
		BuildForest.main(arg2);
		
		String []  arg3 = {"-a",
							"-m",strOutPath+"-builder",
							"-o",outPath,
							"-ds",strOutPath+".info",
							"-i",strInPath,
							"-mr"};
		TestForest.main(arg3);
		
	}
}

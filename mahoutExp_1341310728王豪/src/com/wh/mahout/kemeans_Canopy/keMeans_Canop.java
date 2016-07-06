package com.wh.mahout.kemeans_Canopy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class keMeans_Canop {
	static Configuration conf = new Configuration();
	static String inputPath = "hdfs://master:9000/usr/wh/input/CanopyAndKmeans/kmeansdata";
	static String outPutPath ="hdfs://master:9000/usr/wh/output/CanopyAndKmeans/OutPut";
	static String tmpPath = "hdfs://master:9000/usr/wh/temp/CanopyAndKmeans/Tmp";
	static String tmpPathk = "hdfs://master:9000/usr/wh/temp/KmeansTmp";
	static Path  input  = new Path(inputPath);
	static Path  output  = new Path(outPutPath);
	static Path  directoryInput  = new Path(output+"_vw","data");
	public static void main(String[] args){
		try {
			System.out.println("------------------Canopy----------------");
			keMeans_Canop.canopy();
			System.out.println("------------------K-Means----------------");
			keMeans_Canop.kmeans();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void canopy() throws Exception{
		//Configuration conf = new Configuration();
		HadoopUtil.delete(conf,directoryInput);
		HadoopUtil.delete(conf, output);
		InputDriver.runJob(input, directoryInput, "org.apache.mahout.math.RandomAccessSparseVector");
		String[] arg = new String[]{
			"-i",directoryInput.toString(),//inputPath doc xu lie hua
			"-o",outPutPath,
			"-ow",
			"-t1","8000","-t2","2005","-t3","8000","-t4","2005",
			"-cl",	//fen lie
			"--tempDir",tmpPath
		};
		CanopyDriver job = new CanopyDriver();
		ToolRunner.run(conf, job,arg);
		ClusterDumper clusterDumper1 = new ClusterDumper(new Path(outPutPath,"clusters-*-final"),new Path(outPutPath,"clusteredPoints-out"));
		clusterDumper1.printClusters(null);
	}
	public static void kmeans () throws Exception{
		Path outputk = new Path("hdfs://master:9000/usr/wh/output/KmeansOutPut");
		HadoopUtil.delete(conf, outputk);
		String[] argsk = new String []{
				"-i",directoryInput.toString(),
				"-c",outPutPath +"/"+Cluster.INITIAL_CLUSTERS_DIR+"-final",
				"-o",outputk.toString(),
				"-ow",
				"-x","10",
				"-cl",
				"--tempDir",tmpPathk
		};
		KMeansDriver kms = new KMeansDriver();
		ToolRunner.run(conf, kms,argsk);
		HadoopUtil.delete(conf, new Path(tmpPath));
		HadoopUtil.delete(conf, new Path(tmpPathk));
		ClusterDumper clusterDumper_k = new ClusterDumper(new Path(outputk,"clusters-*-final"),new Path(outputk,"clusteredPoints-out"));
		clusterDumper_k.printClusters(null);
	}
}

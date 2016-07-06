package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class CanopyImpl
{
	public static void run() throws Exception
	{
		String strInPath="hdfs://master:9000/usr/wh/input/CanopyAndKmeans/kmeansdata";
		String strOutPath="hdfs://master:9000/usr/wh/output/CanopyAndKmeans/1341310728";
		String tmpPath="hdfs://master:9000/usr/wh/temp/CanopyAndKmeans/1341310728";

		Path input = new Path(strInPath);
		Path output = new Path(strOutPath);
		Path directoryContainingConvertedInput = new Path(output + "_vw", "data");

		Configuration conf=new Configuration();
		HadoopUtil.delete(conf, directoryContainingConvertedInput);
		HadoopUtil.delete(conf, output);

		//	InputDriver.runJob用于将原始数据文件转换成 Mahout进行计算所需格式的文件 SequenceFile
		InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
		String[] arg=new String[]{
				"-i",directoryContainingConvertedInput.toString(),	//	输入路径，序列文件
				"-o",strOutPath,									//	输出路径
				"-ow",												//	覆盖已存在的路径
				"-t1","8000", "-t2","2005", "-t3","8000", "-t4","2005",//聚类的半径，半径越小类越多，成对出现
				"-cl",												//	对数据进行分类
				"--tempDir",tmpPath};

			CanopyDriver job=new CanopyDriver();
			//	用Canopy算法确定初始簇的个数和簇的中心
			ToolRunner.run(conf, job, arg);

			ClusterDumper clusterDumper1 = new ClusterDumper(new Path(strOutPath, "clusters-*-final"),
					new Path(strOutPath, "clusteredPoints-out"));
		    clusterDumper1.printClusters(null);
		    
		    
//kmeans算法开始。。。
		    Path outputk = new Path("hdfs://master:9000/usr/wh/output/CanopyAndKmeans/1341310822Kmeans");
		    HadoopUtil.delete(conf, outputk);
		    String tmpPathk = "hdfs://master:9000/usr/wh/temp/CanopyAndKmeans/1341310822Kmeans";
		    String[] argk=new String[]{
					"-i",directoryContainingConvertedInput.toString(),				//	输入路径，序列文件
					"-c",strOutPath + "/" + Cluster.INITIAL_CLUSTERS_DIR +"-final",	//	初始聚类中心文件
					"-o",outputk.toString(),										//	输出路径
					"-ow",
					"-x","10",														//	最大迭代次数
					"-cl",
					"--tempDir",tmpPathk};
		    KMeansDriver kmjob = new KMeansDriver();
		    ToolRunner.run(conf, kmjob, argk);
		    
		    HadoopUtil.delete(conf, new Path(tmpPath));
		    HadoopUtil.delete(conf, new Path(tmpPathk));

		    // ClusterDumper类将聚类的结果装换并写出来
		    ClusterDumper clusterDumper = new ClusterDumper(new Path(outputk, "clusters-*-final"),
		    		new Path(outputk, "clusteredPoints-out"));
		    clusterDumper.printClusters(null);
	}
	public static void main(String []args) throws Exception{
		run();
	}
}
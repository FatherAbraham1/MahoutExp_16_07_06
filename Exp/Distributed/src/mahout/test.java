package mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.common.HadoopUtil;

public class test {
	public static void main(String[] args) throws Exception {
		String StrInPath="hdfs://master:9000/usr/wh/input/Distributed/item";
		String StrOutPath="hdfs://master:9000/usr/wh/output/Distributed/1341310728";
		
		String tempPath="hdfs://master:9000/usr/wh/temp/Distributed/1341310728";
		String [] arg=new String[]{
				"-i",StrInPath,
				"-o",StrOutPath,
				"-n","3",
				"-b","false",
				"-s","SIMILARITY_EUCLIDEAN_DISTANCE",
				"--maxPrefsPerUser","7",
				"--minPrefsPerUser","2",
				"--maxPrefsInItemSimilarity","7",
				"--tempDir",tempPath
		};
		Configuration conf=new Configuration();
		HadoopUtil.delete(conf, new Path(StrOutPath));
		HadoopUtil.delete(conf, new Path(tempPath));
		RecommenderJob job=new RecommenderJob();
		ToolRunner.run(conf, job, arg);
	}

}

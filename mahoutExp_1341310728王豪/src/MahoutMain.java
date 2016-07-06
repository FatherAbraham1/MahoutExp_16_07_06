import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import org.apache.mahout.common.HadoopUtil;

public class MahoutMain {
	public static void main(String [] args){
		try{
			dimrun();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void dimrun() throws Exception{
		String strInPath = "hdfs://master:9000/usr/wh/input/Distributed/item";
		String strOutPath = "hdfs://master:9000/usr/wh/output/Distributed/1341310728";
		String tmpPath =  "hdfs://master:9000/usr/wh/temp/Distributed/1341310728";

		String[] arg = new String []{
				"-i",strInPath,
				"-o",strOutPath,
				"-n","3",
				"-b","false",
				"-s","SIMILARITY_EUCLIDEAN_DISTANCE",
				"--maxPrefsPerUser","7",
				"--minPrefsPerUser","2",
				"--maxPrefsInItemSimilarity","7",
				"--tempDir",tmpPath};
		Configuration conf  = new Configuration();
		HadoopUtil.delete(conf, new Path(strOutPath));
		RecommenderJob job = new RecommenderJob();
		ToolRunner.run(conf, job,arg);
	}
}

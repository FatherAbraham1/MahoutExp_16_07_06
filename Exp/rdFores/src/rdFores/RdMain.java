package rdFores;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.mapreduce.BuildForest;
import org.apache.mahout.classifier.df.mapreduce.TestForest;
import org.apache.mahout.classifier.df.tools.Describe;
import org.apache.mahout.common.HadoopUtil;

public class RdMain {
	public static void main(String[] args) throws Exception {
		String StrInPath="hdfs://master:9000/usr/wh/input/RDFores/rfdata";
		String StrOutPath="hdfs://master:9000/usr/wh/output/RDFores/1341310728";
		String outPath="hdfs://master:9000/usr/wh/temp/RDFores/1341310728";
		
		Configuration conf=new Configuration();
		HadoopUtil.delete(conf, new Path(StrOutPath));
		String buildPath=StrOutPath+"-builder";
		HadoopUtil.delete(conf, new Path(buildPath));
		HadoopUtil.delete(conf, new Path(outPath));
		
		String[] pa={"-d","I","9","N","L",
				                "-p",StrInPath,
				                "-f",StrOutPath+".info"};
		HadoopUtil.delete(conf, new Path(pa[Arrays.asList(pa).indexOf("-f")+1]));
		Describe.main(pa);
		
		String[] arg2={"-d",StrInPath,
				                    "-t","5",
				                    "-o",StrOutPath+"-builder",
				                    "-sl","3",
				                    "-ds",StrOutPath+".info",
				                    "-ms","3"};
		BuildForest.main(arg2);
		
		String[] arg3={"-a",
                                   "-m",StrOutPath+"-builder",
                                   "-o",outPath,
                                   "-ds",StrOutPath+".info",
                                   "-i",StrInPath,
                                   "-mr"};
		TestForest.main(arg3);
	}

}

package fp;

import java.io.IOException;

public class parserResult {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		boolean flag=Parser.Parser("hdfs://master:9000/outOFgxy/outputKmeans/clusteredPoints/part-m-00000",
				"hdfs://master:9000/outOFgxy/outputKmeans/result", "master", new AKVRegex());
		System.out.println(flag);	
	}
}

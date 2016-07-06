package fp;

import org.apache.mahout.fpm.pfpgrowth.FPGrowthDriver;

public class test {
	public static void main(String[] args) {
		test t=new test();
		t.run();
	}
	String StrInPath="hdfs://master:9000/usr/wh/input/FP/fpdata";
	String StrOutPath="hdfs://master:9000/usr/wh/output/FP/1341310728";
	public String run(){
		String tempPath="hdfs://master:9000/usr/wh/temp/FP/1341310728";
		String [] arg=new String[19];
		arg[0]="-Dmapred.reduce.tasks="+2;
		arg[1]="-i";
		arg[2]=StrInPath;
		arg[3]="-o";
		arg[4]=StrOutPath;
		arg[5]="-method";
		arg[6]="mapreduce";
		arg[7]="-regex";
		arg[8]="，";
		arg[9]="-g";
		arg[10]=String.valueOf(7);
		arg[11]="-s";
		arg[12]=String.valueOf(3);
		arg[13]="-k";
		arg[14]=String.valueOf(50);
		arg[15]="--tempDir";
		arg[16]=tempPath;
		arg[17]="-tc";
		arg[18]=String.valueOf(5);
		try
		{
//			FPGrowthDriver.main(arg);
			FPParser(StrOutPath);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return "ok";
	}
	
	private boolean FPParser (String outPath) {
		boolean flag=false;
		try
		{
			flag=Parser.Parser(outPath+"/frequentpatterns/part-r-00003", 
					outPath+"/result", "master",new AKVRegex());
			System.out.println(flag);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return flag;
	}

}

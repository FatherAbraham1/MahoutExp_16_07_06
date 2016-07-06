package com.wh.mahout.mahout_FP;

import java.io.IOException;

import org.apache.mahout.fpm.pfpgrowth.FPGrowthDriver;

public class FP_Tree {
	String strInPath = "hdfs://master:9000/usr/wh/input/FP/fpdata";
	String strOutPath = "hdfs://master:9000/usr/wh/input/FP/1341310728";
	public static void main(String[] args){
		try{
			new FP_Tree().run();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public String run() throws Exception{
		String tmpPath = "hdfs://master:9000/usr/wh/input/FP/1341310728";
		String [] args = new String[19];
		args[0]="-Dmapred.reduce.tasks="+2; //3
		args[1]="-i";
		args[2]=strInPath;
		args[3]="-o";
		args[4]=strOutPath;
		args[5]="-method";    //diaoyonggaisuanfafangshishibingxinghaishidanji
		args[6]="mapreduce";
		args[7]="-regex";    //fengefu
		args[8]="，";
		args[9]="-g";    //groupNums.jinzaiM/Rmoshishiyong
		args[10]=String.valueOf(7);
		args[11]="-s";
		args[12]=String.valueOf(3);
		args[13]="-k";
		args[14]=String.valueOf(50);
		args[15]="--tempDir";
		args[16]=tmpPath;
		args[17]="-tc";
		args[18]=String.valueOf(5);
		try{
			//fp suanfa
			FPGrowthDriver.main(args);
			FPParser(strOutPath);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return "ok";
		
	}

private boolean FPParser(String outPath){
		
		boolean flag = false;
		try{
			flag = Parser.Parser(outPath+"/frequentpatterns/part-r-00000",outPath+"/result","master", new AKVRegex());
		}catch(IOException e){
			e.printStackTrace();
		}
		return flag;
	}
}

package gsChild4;

import java.io.*;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang.StringUtils;

public class MapTaskOne extends Mapper<LongWritable, Text, LongWritable, Text>{
	
	public static final int uLimit = 10;
	public static final double interval = 0.1;
	public static final double bwThold = 0.23;
	private Text word = new Text();
	private LongWritable gid = new LongWritable();
	
    public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		String line = values.toString();
		
		double sum = 0.0, avg=0.0,nom=0.0;
		int temp2 = 0, i=0,j,clnum,bwr=0,tmCls;
		int pSet,rNum;
		long temp4;
		
		temp4 = key.get();
		
		Configuration conf =context.getConfiguration();
		int samples = conf.getInt("smnum",62); 
		int trNum = conf.getInt("trNum", 20);
		int tstNum = conf.getInt("tsNum", 42);
		clnum  = conf.getInt("clsNum",2);
		
		String smstr = conf.get("smpls"); 
		String[] smp_cls = StringUtils.split(smstr,"=");
		
		String trcStr = conf.get("trCls"); 
		String[] clsCount = StringUtils.split(trcStr,",");
		
		String[] rgSetArr = StringUtils.split(conf.get("rgSet"),",");
		
		double[] exprArr = new double[samples];
		double[] cAvg = new double[clnum];
		double[] dnom = new double[clnum];
	
		int temp3 = (int) ((temp4/931)+1+0.2);
		
		//updated code from here
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while (tokenizer.hasMoreTokens()) {
			
			exprArr[i] = Double.parseDouble(tokenizer.nextToken());
			if(i<trNum){
				//BW measurement
				sum += exprArr[i];
				temp2 =Integer.parseInt(smp_cls[i]);
				cAvg[temp2-1] += exprArr[i]; 
			}
			i++;
			
		}
		
		for(j=0;j<clnum;j++){
			cAvg[j] = cAvg[j]/Integer.parseInt(clsCount[j]);
			dnom[j] = 0;
		}
		avg = sum / trNum;
		
		for(i=0; i<trNum; i++){
			tmCls = Integer.parseInt(smp_cls[i]);
			dnom[tmCls-1] += Math.pow((exprArr[i]-cAvg[tmCls-1]), 2);
		}
		
		double dnomT = 0;
		for(j=0;j<clnum;j++){
			nom += Integer.parseInt(clsCount[j])*Math.pow((cAvg[j]-avg), 2);
			dnomT += dnom[j];
		}
		
		double bwrD = nom / dnomT;
		bwr = (int)(bwrD*10);
		
		 //tasks start for successful bwr
		if(bwrD>=bwThold){
			
			bwr = (bwr>=10)?10:bwr;
			word.set(new byte[0]);
			
			Random Generator = new Random();
			int randN = Generator.nextInt(Integer.parseInt(rgSetArr[bwr-1]))+1;
			
			long temp5=0;
			
			for(int k=0; k<tstNum;k++){
				
				for(i=0; i<trNum; i++){
					temp5 = ( ( (k+1)*1000 +(i+1) )*10000) + temp3;
					gid.set(temp5);
					//word.set(String.valueOf(exprArr[k+trNum])+" "+String.valueOf(exprArr[i])
					//		+" "+conf.get("gToSet-"+bwr+"-"+randN) );
					word.set(String.valueOf(exprArr[k+trNum])+" "+String.valueOf(exprArr[i])
							+" "+"gToSet-"+bwr+"-"+randN );
					context.write(gid,word);
					
				}
			}
		 }
		 //tasks end for successful bwr
    }

}

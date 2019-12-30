import gsChild4.MapTaskOne;
import gsChild4.MapTaskTwo;
import gsChild4.RedTaskTwo;
import gsChild4.MapTaskThree;
import gsChild4.RedTaskThree;
import gsChild4.MapTaskFour;
import gsChild4.RedTaskFour;
import gsChild4.MapTaskFive;
import gsChild4.RedTaskFive;

import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import java.util.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class GsHadoop4 extends Configured {

	static final long MEGABYTE = 1024*1024;
	public static final int samples = 62;
	public static final int classNum = 2;
	public static final int genes = 2000;
	public static final int training = 20;
	public static final int test = 42;
	//public static final int test = 3;
	public static final int psetNum = 1000;
	public static final int psetLen = 80;
	public static final int gpNum = 10;
	//public static final float acThold = 0.83;
	
	
	public static int[] trClsNum = new int[classNum];
	public static int[] smplCls;
	public static int[] weight = new int[]{45,90,100,110,120,130,140,150,160,175};
	public static int[] rGtoSet = new int[]{45,100,90,80,70,60,50,40,30,20};
	
	public static String path0 = "/user/vm/tohid/colon"; 
	public static String path1 = "/user/vm/tohid/gsh-4-1-5"; 
	public static String path2 = "/user/vm/tohid/gsh-4-2-5"; 
	public static String path3 = "/user/vm/tohid/gsh-4-3-5"; 
	public static String path4 = "/user/vm/tohid/gsh-4-4-5"; 
	public static String path5 = "/user/vm/tohid/gsh-4-5-5"; 
	
	
	//add sample info and class label
	public static void sample_info(){
		File file = new File(System.getProperty("user.dir") + "/src/tissues.txt");
		int i;
		smplCls = new int[samples];
		
		try {
			Scanner scanner = new Scanner(file);
		    for(i=0; i<samples; i++){
		        if(scanner.hasNextInt()){
		            smplCls[i] = (scanner.nextInt()>0)? 1: 2; 
		            if(i<training){
		            	trClsNum[smplCls[i]-1]++;
		            }
		        }	
            }
            scanner.close();
            //System.out.println(Arrays.toString(smplCls));
		} catch (Exception e) {
		        System.out.println(e);
	    }
		return;
	}
	
	public static String implodeArray(int[] inputArray, String glueString) {

		String output = "";

		if (inputArray.length > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append(inputArray[0]);

			for (int i=1; i<inputArray.length; i++) {
				sb.append(glueString);
				sb.append(inputArray[i]);
			}

			output = sb.toString();
		}
		return output;

	}

	public static void main(String[] args) throws Exception{
				
		int i,j,rNum;
		sample_info();
		
		Configuration conf = new Configuration();
		
		conf.addResource("hdfs-default.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-default.xml");
		conf.addResource("mapred-site.xml");	
		
		//randome gene -> setlist 
		Random Generator = new Random();
		for(int range=1; range<weight.length;range++){
			
			for(i=0;i<rGtoSet[range];i++){
				int[] rGset = new int[weight[range]];
				for(j=0; j<weight[range]; j++){
					rNum = Generator.nextInt(psetNum)+1;
					rGset[j] = rNum;
				}
				conf.set("gToSet-"+(range+1)+"-"+(i+1), implodeArray(rGset,","));
			}
			
		}
		
		
		//set global variables for samples		
		conf.setInt("smnum",samples);
		conf.setInt("gnum",genes);
		conf.setInt("clsNum",classNum);		
		conf.setInt("trNum",training);		
		conf.setInt("tsNum",test);		
		conf.set("smpls", implodeArray(smplCls,"="));
		conf.set("trCls", implodeArray(trClsNum,","));
		conf.set("rgSet", implodeArray(rGtoSet,","));
		
		Job job = new Job(conf);
		job.setJobName("GeSelection");
		job.setJarByClass(GsHadoop4.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapTaskOne.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job, new Path(path0) );
		TextOutputFormat.setOutputPath(job, new Path(path1) );
		
		//set split size
		TextInputFormat.setMinInputSplitSize(job, 500);
		TextInputFormat.setMaxInputSplitSize(job, 100*931);
		
		Date startTime = new Date();
		System.out.println("Job started: " + startTime);	
		int exitCode = job.waitForCompletion(true)?0:1;
		
		if(exitCode==0) {	
			Date end_time = new Date();
			System.out.println("Job ended: " + end_time);
			System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");						
		
			
			//2nd MR driver code starts
			//conf.setFloat("acThold",acThold);		
			
			Job job2 = new Job(conf);
			job2.setJobName("GeSelectionMR2");
			job2.setJarByClass(GsHadoop4.class);
			
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);
					
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setMapperClass(MapTaskTwo.class);
			job2.setReducerClass(RedTaskTwo.class);
			
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			TextInputFormat.setInputPaths(job2, new Path(path1) );
			TextOutputFormat.setOutputPath(job2, new Path(path2) );
			
			//set split size
			TextInputFormat.setMinInputSplitSize(job2, 100);
			TextInputFormat.setMaxInputSplitSize(job2, 100*1024);
		
			int eCode2 = job2.waitForCompletion(true) ? 0 : 1;
			
			if(eCode2 == 0){
				Date end_time2 = new Date();
				System.out.println("Job ended: " + end_time2);
				System.out.println("The job took " + (end_time2.getTime() - startTime.getTime()) /1000 + " seconds.");						
				
				
				//3rd MR driver code starts
				Job job3 = new Job(conf);
				job3.setJobName("GeSelectionMR3");
				job3.setJarByClass(GsHadoop4.class);
				
				job3.setMapOutputKeyClass(LongWritable.class);
				job3.setMapOutputValueClass(Text.class);
						
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				
				job3.setMapperClass(MapTaskThree.class);
				job3.setReducerClass(RedTaskThree.class);
				
				job3.setInputFormatClass(TextInputFormat.class);
				job3.setOutputFormatClass(TextOutputFormat.class);
				
				TextInputFormat.setInputPaths(job3, new Path(path2) );
				TextOutputFormat.setOutputPath(job3, new Path(path3) );
				
				//set split size
				TextInputFormat.setMinInputSplitSize(job3, 100);
				TextInputFormat.setMaxInputSplitSize(job3, 4*1024*1024);
			
				int eCode3 = job3.waitForCompletion(true) ? 0 : 1;
				
				if(eCode3 == 0){
					Date end_time3 = new Date();
					System.out.println("Job ended: " + end_time3);
					System.out.println("The job took " + (end_time3.getTime() - startTime.getTime()) /1000 + " seconds.");						
				
					
					//4th MR driver code starts
					Job job4 = new Job(conf);
					job4.setJobName("GeSelectionMR4");
					job4.setJarByClass(GsHadoop4.class);
					
					job4.setMapOutputKeyClass(IntWritable.class);
					job4.setMapOutputValueClass(Text.class);
							
					job4.setOutputKeyClass(Text.class);
					job4.setOutputValueClass(Text.class);
					
					job4.setMapperClass(MapTaskFour.class);
					job4.setReducerClass(RedTaskFour.class);
					
					job4.setInputFormatClass(TextInputFormat.class);
					job4.setOutputFormatClass(TextOutputFormat.class);
					
					TextInputFormat.setInputPaths(job4, new Path(path3) );
					TextOutputFormat.setOutputPath(job4, new Path(path4) );
					
					//set split size
					TextInputFormat.setMinInputSplitSize(job4, 1024);
					TextInputFormat.setMaxInputSplitSize(job4, 50*1024);
				
					int eCode4 = job4.waitForCompletion(true) ? 0 : 1;
					
					if(eCode4 == 0){
						Date end_time4 = new Date();
						System.out.println("Job ended: " + end_time4);
						System.out.println("The job took " + (end_time4.getTime() - startTime.getTime()) /1000 + " seconds.");						
				
						//5th MR driver code starts
						Job job5 = new Job(conf);
						job5.setJobName("GeSelectionMR5");
						job5.setJarByClass(GsHadoop4.class);
						
						job5.setMapOutputKeyClass(IntWritable.class);
						job5.setMapOutputValueClass(IntWritable.class);
								
						job5.setOutputKeyClass(Text.class);
						job5.setOutputValueClass(Text.class);
						
						job5.setMapperClass(MapTaskFive.class);
						job5.setCombinerClass(RedTaskFive.class);
						job5.setReducerClass(RedTaskFive.class);
						
						job5.setInputFormatClass(TextInputFormat.class);
						job5.setOutputFormatClass(TextOutputFormat.class);
						
						TextInputFormat.setInputPaths(job5, new Path(path4) );
						TextOutputFormat.setOutputPath(job5, new Path(path5) );
						
						//set split size
						TextInputFormat.setMinInputSplitSize(job5, 10);
						TextInputFormat.setMaxInputSplitSize(job5, 5*1024);
					
						int eCode5 = job5.waitForCompletion(true) ? 0 : 1;
						
						if(eCode5 == 0){
							Date end_time5 = new Date();
							System.out.println("Job ended: " + end_time5);
							System.out.println("The job took " + (end_time5.getTime() - startTime.getTime()) /1000 + " seconds.");						
						}
						else{
							System.out.println("5th MR Job Failed!!!");
						}
					}
					else{
						System.out.println("4th MR Job Failed!!!");
					}
				
				}
				else{
					System.out.println("3rd MR Job Failed!!!");
				}
				 
				
			}	
			else{
				System.out.println("2nd MR Job Failed!!!");
			}
			
		}
		else{
			System.out.println("1st MR Job Failed!!!");
		}
		
	}
	
	
}

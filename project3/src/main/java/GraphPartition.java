import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    // Intializing all the variables in the constructor Vertex to 0 at the start 
    Vertex(){
    	this.id =0;
    	this.adjacent = new Vector<Long>();
    	this.centroid = 0;
    	this.depth = 0;
    }
    // this constructor is storing specific argumesnts too the variables in the Vertex class
    Vertex(long id, Vector<Long> adjacent, long centroid, short depth){
    	this.id = id;
    	this.adjacent = adjacent;
    	this.centroid = centroid;
    	this.depth = depth;
    }
    // this method is for readFields that takes a DataInput object as an argument and reads data from it to initialize the instance variables for Vertex
    public void readFields(DataInput in) throws IOException,EOFException {
		// TODO Auto-generated method stub
		this.id = in.readLong();
		this.centroid = in.readLong();
		this.depth = in.readShort();
		this.adjacent = new Vector<Long>();
		int read = in.readInt();
		for(int i=0;i<read;i++) {
			this.adjacent.add(in.readLong());
		}
		
	}
    // this method is for readFields that takes a DataOutput object as an argument and Writes data from it to initialize the instance variables for Vertex
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.id);
		out.writeLong(this.centroid);
		out.writeShort(this.depth);
		out.writeInt(this.adjacent.size());
		for(int j=0;j<this.adjacent.size();j++) {
			out.writeLong(this.adjacent.get(j));
		}
		
	}
	
    
}  

public class GraphPartition {
    static Vector<Long> centroid_size = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;

    public static class FirstMapper_READ extends Mapper<Object,Text,LongWritable,Vertex>{
    	public void map(Object key, Text line, Context context) throws IOException, InterruptedException{
    		String s = line.toString();
    		String[] values = s.split(",");
    		Vertex v1 = new Vertex();
    		v1.id  = Long.parseLong(values[0]);
            // take the first 10 vertices of each split to be the centroids
    		for(int i=1;i<values.length;i++) {
    			v1.adjacent.addElement(Long.parseLong(values[i]));
    		}
    		
    		LongWritable id = new LongWritable(v1.id);
    		long centroid_temp = -1;
    		if(centroid_size.size()<10) {
    			centroid_size.add(v1.id);
    			centroid_temp = v1.id;
    		}
    		else {
    			centroid_temp = -1;
    		}
    		context.write(id,new Vertex(v1.id, v1.adjacent,centroid_temp,(short)0));
    	}
    }

    public static class SecondMapper_BFS extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{
    	public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException{
    		context.write(new LongWritable(vertex.id), vertex);
    		if(vertex.centroid>0) {
    			for(Long n: vertex.adjacent) {
    				context.write(new LongWritable(n),new Vertex(n,new Vector<Long>(),vertex.centroid,BFS_depth));
    			}
    		}
    	}
    }

    public static class FirstReducer_READ extends Reducer<LongWritable,Vertex,LongWritable, Vertex>{
    	public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException{
    		short min_depth = 1000;
    		long id = key.get();
    		Vertex m = new Vertex(id, new Vector<Long>(),-1,(short)0);
    		for(Vertex v:values) {
    			if(!v.adjacent.isEmpty()) {
    				m.adjacent = v.adjacent;
    				
    			}
    			if(v.centroid>0 && v.depth<min_depth) {
    				min_depth = v.depth;
    				m.centroid = v.centroid;
    			}
    			m.depth = min_depth;
    		}    		
    		context.write(key, m);
    	}
    }

    public static class ThirdMapper_CAL extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
    	public void map(LongWritable id, Vertex value, Context context) throws IOException, InterruptedException{
    		context.write( new LongWritable(value.centroid),new LongWritable(1));
    	}
    }

    public static class SecondReducer_BFS extends Reducer<LongWritable,LongWritable,LongWritable, LongWritable>{
    	public void reduce(LongWritable centroid, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
    		int m = 0;
    		for(LongWritable v:values) {
    			m+=v.get();
    		}
    		context.write(new LongWritable(centroid.get()), new LongWritable(m));
    	}
    }

    public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("MapReduce1");
        
        job1.setJarByClass(GraphPartition.class);
        job1.setMapperClass(FirstMapper_READ.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/i0"));
        job1.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            Job job2 = Job.getInstance();
            job2.setJobName("MapReduce2");
	    	job2.setJarByClass(GraphPartition.class);
            job2.setMapperClass(SecondMapper_BFS.class);
            job2.setReducerClass(FirstReducer_READ.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(job2, new Path(args[1]+"/i"+i));
            SequenceFileOutputFormat.setOutputPath(job2, new Path(args[1]+"/i"+(i+1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        job3.setJobName("MapReduce3");
        job3.setJarByClass(GraphPartition.class);
		job3.setMapperClass(ThirdMapper_CAL.class);
        job3.setReducerClass(SecondReducer_BFS.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }
}
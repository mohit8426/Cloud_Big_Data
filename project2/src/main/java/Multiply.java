import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Triple implements Writable {
    public int tag;
    public int index;
    public double value;
     /* write your code here */

    Triple () {

    }
    Triple(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
        }
      
        public void write(DataOutput out) throws IOException {
            
            out.writeInt(tag);
            out.writeInt(index);
            out.writeDouble(value);
        }
        // public int getFirst() {
        //     return 0;
        // }
        // public double getThird() {
        //     return 0;
        // }
        // public int getSecond() {
        //     return 0;
        // }

        public void readFields(DataInput in) throws IOException {
            tag = in.readInt();
            index = in.readInt();
            value = in.readDouble();
        }       
}

class Pair implements WritableComparable<Pair> {
  
public int i;
public int j;
/* write your code here */
Pair() {
    i=0;
    j=0;
}

Pair(int i, int j) {
    this.i = i;
    this.j = j;
}

@Override
public void write(DataOutput out) throws IOException {
    out.writeInt(i);
    out.writeInt(j);
}

@Override
public void readFields(DataInput in) throws IOException {
    i = in.readInt();
    j = in.readInt();
}

@Override
public int compareTo(Pair pair1) {
    if (i > pair1.i) {
        return 1;
    } else if (i < pair1.i) {
        return -1;
    } else {
        if (j > pair1.j) {
            return 1;
        } else if (j < pair1.j) {
            return -1;
        } else {
            return 0;
        }
    }
}

public String toString() {
     return i + " " + j + " ";
}
   


    
}

public class Multiply {

    /* write your code here */
        public static class FirstMapper_M extends Mapper<Object,Text,IntWritable,Triple> {
        @Override
        public void map ( Object key, Text line , Context context )
        throws IOException, InterruptedException {
        Scanner s = new Scanner(line.toString()).useDelimiter(",");    
        
        int i = s.nextInt();
        int k = s.nextInt();
        double m = s.nextDouble();
        

        
        context. write (new IntWritable(k),new Triple(0,i,m));
        s.close();
        }

        // private DoubleWritable Triple(int i, int i2, double m) {
        //     return null;
        // }
        }
            public static class FirstMapper_N extends Mapper<Object,Text,IntWritable,Triple> {
            @Override
            public void map ( Object key, Text line , Context context )
            throws IOException, InterruptedException {
            Scanner s = new Scanner(line.toString()).useDelimiter(",");    
            int k = s.nextInt();
            int j = s.nextInt();
            double n = s.nextDouble();
    
            
            context. write (new IntWritable(k),new Triple(1,j,n));
            s.close();
            }
        }
        // private DoubleWritable Triple(int i, int j, double n) {
        //         return null;
        //     }
            public static class FirstReducer extends Reducer<IntWritable,Triple,Pair,DoubleWritable>
            {
            public void reduce ( IntWritable key, Iterable<Triple> values, Context context )
            throws IOException, InterruptedException {
            List<Triple> Triple1 = new ArrayList<Triple>();
            List<Triple> Triple2 = new ArrayList<Triple>();
            Triple1.clear();
            Triple2.clear();

            Configuration config = context.getConfiguration();
            for (Triple v: values) {
                Triple temp1 = ReflectionUtils.newInstance(Triple.class, config);
                ReflectionUtils.copy(config, v, temp1);
                if (temp1.tag == 0) {
                    Triple1.add(temp1);
                } else {
                    Triple2.add(temp1);
                }
            }
                
            for ( Triple t1: Triple1 ) {

                for ( Triple t2: Triple2 ) {

                    context.write(new Pair(t1.index,t2.index),new DoubleWritable(t1.value*t2.value));

                }
            }



            }

            // private DoubleWritable Double(double d) {
            //     return null;
            // }
            }

            public static class SecondMapper extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable> {
                public void map(Pair pair, DoubleWritable value, Context context) throws IOException, InterruptedException {
               
                  context.write(pair, value);
                }
              }
              
            
            public static class SecondReduces extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
                public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
              
                  double sum = 0.0;
                  for (DoubleWritable value1 : values) {
                    sum = sum + value1.get();
                  }
              
             
                  context.write(pair, new DoubleWritable(sum));
                }
              }

        public static void main ( String[] args ) throws Exception {
        /* write your code here */
        Job job = Job.getInstance();
        job.setJobName("Map_Reduce1");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Triple.class);
        job.setReducerClass(FirstReducer.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,FirstMapper_M.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,FirstMapper_N.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

        Job job1 = Job.getInstance();
        job1.setJobName("Map_Reduce2");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(Pair.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setMapperClass(SecondMapper.class);
        job1.setReducerClass(SecondReduces.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[2]));
        FileOutputFormat.setOutputPath(job1,new Path(args[3]));
        job1.waitForCompletion(true);
    }

}

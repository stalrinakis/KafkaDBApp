package test.pack;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class kmeans123 {
    public static void main() throws Exception {
        Path input = new Path("C:\\Uni\\eclipse\\projects\\FirstApp\\src\\main\\resources\\kmeans\\input.txt");
        Path output = new Path("C:\\Uni\\eclipse\\projects\\FirstApp\\src\\main\\resources\\kmeans\\output");
        
        
        Configuration conf = new Configuration();
        
        run(conf,input,output,new EuclideanDistanceMeasure(), 3, 0.5, 10);
        
    }
    
    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k, double convergenceDelta, int maxInterations)throws Exception {
    	
    	
    	Path directoryContainingConvertedInput = new Path(output,"KmeansOutputData");
    	InputDriver.runJob(input, directoryContainingConvertedInput,"org.apache.mahout.math.RandomAccessSparseVector");
    	
    	Path clusters = new Path("random-seeds");
    	clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
    			
    			KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta,maxInterations, true, 0.0, false );
    			
    			Path outGlob = new Path(output, "clusters-*-final");
    			Path clusteredPoints = new Path(output,"clusteredPoints");
    			
    			ClusterDumper cd = new ClusterDumper(outGlob,clusteredPoints);
    			cd.printClusters(null);
    			
    }
}
 
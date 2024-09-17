package test.pack;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class KMeansCluster {
	public static void kmeanscl(StringBuilder kmeansInput) throws Exception {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);

	    // Create a temporary sequence file
	    Path seqFilePath = new Path("temp.seq");
	    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, seqFilePath,
	            Text.class, double[].class);

	    // Convert the StringBuilder to a BufferedReader
	    BufferedReader br = new BufferedReader(new StringReader(kmeansInput.toString()));

	    String line;
	    while ((line = br.readLine()) != null) {
	        double[] vector = Arrays.stream(line.split(" "))
	                .mapToDouble(Double::parseDouble)
	                .toArray();

	        writer.append(new Text(), vector);
	    }

	    writer.close();

	    Path output = new Path("C:\\Uni\\eclipse\\projects\\FirstApp\\src\\main\\resources\\kmeans\\output");

	    run(conf, seqFilePath, output, new EuclideanDistanceMeasure(), 3, 0.5, 10);
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

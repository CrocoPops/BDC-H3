import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;

public class G008HW3 {

    // After how many items should we stop?
    // public static final int THRESHOLD = 1000000;

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            throw new IllegalArgumentException("USAGE: number of items, frequency of threshold, accuracy parameter, confidence parameter, port number");
        }
        // IMPORTANT: the master must be set to "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("G008HW3");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(10));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore. The
        // main thread will first acquire the only permit available, and then it will try
        // to acquire another one right after spinning up the streaming computation.
        // The second attempt at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met the semaphore is released, basically giving "green light" to the main
        // thread to shut down the computation.

        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        int n = Integer.parseInt(args[0]);
        System.out.println("Number of items = " + n);
        float phi = Float.parseFloat(args[1]);
        if(phi <= 0 || phi >= 1) {
            throw new IllegalArgumentException("The phi parameter must be in the range (0, 1)");
        }
        System.out.println("Phi = " + phi);
        float epsilon = Float.parseFloat(args[2]);
        if(epsilon <= 0 || epsilon >= 1) {
            throw new IllegalArgumentException("The epsilon parameter must be in the range (0, 1)");
        }
        System.out.println("Epsilon = " + epsilon);
        float delta = Float.parseFloat(args[3]);
        if(delta <= 0 || delta >= 1) {
            throw new IllegalArgumentException("The delta parameter must be in the range (0, 1)");
        }
        System.out.println("Delta = " + delta);
        int portExp = Integer.parseInt(args[4]);
        System.out.println("Receiving data from port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0] = 0L;
        ArrayList<ArrayList<Tuple2<Long, Long>>> trueFrequentItems = new ArrayList<>(); // True Frequent Items
        ArrayList<ArrayList<Long>> reservoirSampling = new ArrayList<>(); // Reservoir Sampling
        ArrayList<Hashtable<Long, Long>> stickySampling = new ArrayList<>(); // epsilon-AFI with Sticky Sampling
        // HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        JavaPairRDD<Long, Long> batchItems = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .reduceByKey(Long::sum);

                        // True Frequent Items
                        if(batchSize > 0) {
                            trueFrequentItems.add(new ArrayList<>(trueFrequentItems(batchItems, phi, batchSize)));
                            reservoirSampling.add(new ArrayList<>(reservoirSampling(batchItems, phi)));
                            stickySampling.add(new Hashtable<>(stickySampling(batchItems, epsilon, delta, phi, batchSize)));
                        }

                        // If we wanted, here we could run some additional code on the global histogram
                        if (batchSize > 0) {
                            System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                        }
                        if (streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");

        // IMPLEMENTING THE ALGORITHMS
        // True Frequent Items with the threshold phi
        for(ArrayList<Tuple2<Long, Long>> tfi : trueFrequentItems) {
            tfi.sort(Comparator.comparingLong(Tuple2::_2));
            System.out.println("Number of true frequent items = " + tfi.size());
            System.out.println("True Frequent Items = " + tfi);
        }

        // Reservoir Sampling
        for(ArrayList<Long> rs : reservoirSampling) {
            rs.sort(Comparator.comparingLong(Long::longValue));
            System.out.println("Reservoir Sampling = " + rs);
        }
        // epsilon-AFI computed with Sticky Sampling
        for(Hashtable<Long, Long> ss : stickySampling) {
            // sort hashtable by increasing value
            ArrayList<Tuple2<Long, Long>> l = new ArrayList<>();
            ss.forEach((k, v) -> l.add(new Tuple2<>(k, v)));
            l.sort(Comparator.comparingLong(Tuple2::_2));
            System.out.println("Number of sticky sampling items = " + l.size());
            System.out.println("Sticky Sampling Items = " + l);
        }
        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
        System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("Number of items processed = " + streamLength[0]);
        // System.out.println("Number of distinct items = " + histogram.size());
        /*System.out.println("Histogram = " + histogram);
        long max = 0L;
        for (Long key : histogram.keySet()) {
            if (key > max) {max = key;}
        }
        System.out.println("Largest item = " + max);*/
    }

    /**
     * True Frequent Items Algorithm
     * @param stream - stream of items
     * @param phi - frequency threshold
     * @param streamLength - length of the stream
     * @return - list of true frequent items
     */
    public static ArrayList<Tuple2<Long, Long>> trueFrequentItems(JavaPairRDD<Long, Long> stream, float phi, long streamLength) {
        // Round 1
        JavaPairRDD<Long, Long> frequentItems = stream
                .reduceByKey(Long::sum)
                .filter(s -> s._2 >= phi * streamLength);
        // Return the list of frequent items
        return new ArrayList<>(frequentItems.collect());
    }

    /**
     * Reservoir Sampling Algorithm
     * @param stream - stream of items
     * @param phi - frequency threshold used to compute m
     * @return - list of m-sampled items
     */
    public static ArrayList<Long> reservoirSampling(JavaPairRDD<Long, Long> stream, float phi) {
        int m = (int) Math.ceil(1 / phi);
        ArrayList<Long> reservoir = new ArrayList<>();
        int t = 0;
        List<Tuple2<Long, Long>> elements = stream.collect();

        for(Tuple2<Long, Long> el : elements) {
            if (reservoir.size() < m) {
                reservoir.add(el._1);
            } else {
                Random random = new Random();
                if (random.nextFloat() <= (float) m / t) {
                    int i = random.nextInt(m);
                    reservoir.set(i, el._1);
                }
            }
            t++;
        }
        return reservoir;
    }

    /**
     * epsilon-AFI with Sticky Sampling Algorithm
     * @param stream - stream of items
     * @param epsilon - accuracy parameter
     * @param delta - confidence parameter
     * @param phi - frequency threshold
     * @param streamLength - length of the stream
     * @return - hashtable of frequent items with (phi - epsilon) * streamLength probability
     */
    public static Hashtable<Long, Long> stickySampling(JavaPairRDD<Long, Long> stream, float epsilon, float delta, float phi, long streamLength) {
        Hashtable<Long, Long> S = new Hashtable<>();
        double r = Math.log(1 / (delta * phi)) / epsilon;
        List<Tuple2<Long, Long>> elements = stream.collect();

        for(Tuple2<Long, Long> el : elements) {
            Random random = new Random();
            if (S.containsKey(el._1()))
                S.replace(el._1(), el._2(), el._2() + 1);
            else
                if(random.nextDouble() <= r / streamLength)
                    S.put(el._1(), el._2());
        }

        // drop items with frequency less than (phi - epsilon) * streamLength
        S.entrySet().removeIf(entry -> entry.getValue() < (phi - epsilon) * streamLength);
        return S;
    }

}


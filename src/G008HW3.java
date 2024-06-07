import com.google.common.util.concurrent.AtomicDouble;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class G008HW3 {

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


        System.out.println("INPUT PROPERTIES");
        int n = Integer.parseInt(args[0]);
        System.out.print("n = " + n);
        float phi = Float.parseFloat(args[1]);
        if(phi <= 0 || phi >= 1) {
            throw new IllegalArgumentException("The phi parameter must be in the range (0, 1)");
        }
        System.out.print(" phi = " + phi);
        float epsilon = Float.parseFloat(args[2]);
        if(epsilon <= 0 || epsilon >= 1) {
            throw new IllegalArgumentException("The epsilon parameter must be in the range (0, 1)");
        }
        System.out.print(" epsilon = " + epsilon);
        float delta = Float.parseFloat(args[3]);
        if(delta <= 0 || delta >= 1) {
            throw new IllegalArgumentException("The delta parameter must be in the range (0, 1)");
        }
        System.out.print(" delta = " + delta);
        int portExp = Integer.parseInt(args[4]);
        System.out.println(" port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        Hashtable<Long, Long> counterItems[] = new Hashtable[1];
        ArrayList<Long> reservoirSampling[] = new ArrayList[1];
        Hashtable<Long, Long> stickySampling[] = new Hashtable[1];
        streamLength[0] = 0L;
        counterItems[0] = new Hashtable<>();
        reservoirSampling[0] = new ArrayList<>();
        stickySampling[0] = new Hashtable<>();

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < n) {
                        long batchSize = batch.count();
                        long prevStreamLength = streamLength[0];
                        streamLength[0] += batchSize;
                        // Extract the distinct items from the batch
                        List<Long> batchItems = batch
                                .map(Long::parseLong)
                                .collect();
                        // if streamLength[0] is greater than n, maintain only the first n - prevStreamLength elements
                        if(streamLength[0] >= n) {
                            int offset = (int) (n - prevStreamLength);
                            batchItems = batchItems.subList(0, offset);
                            streamLength[0] = n;
                        }
                        // Implementing the algorithms
                        if(batchSize > 0) {
                            // True Frequent Items
                            Hashtable<Long, Long> trueFrequent = trueFrequentItems(batchItems);
                            for(Map.Entry<Long, Long> entry : trueFrequent.entrySet()) {
                                counterItems[0].merge(entry.getKey(), entry.getValue(), Long::sum);
                            }
                            // Reservoir Sampling
                            reservoirSampling(batchItems, reservoirSampling[0], phi, prevStreamLength);
                            // Sticky Sampling
                            stickySampling(batchItems, stickySampling[0], epsilon, delta, phi, n);
                        }
                        if(streamLength[0] >= n) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        sc.start();
        stoppingSemaphore.acquire();

        // True Frequent Items
        System.out.println("EXACT ALGORITHM");
        // return sum of the values of the map
        System.out.println("Number of items in the data structure = " + counterItems[0].size());
        List<Long> trueFrequentItems = new ArrayList<>();
        // Remove items with frequency less than phi * n
        counterItems[0].entrySet().removeIf(entry -> entry.getValue() < phi * n);
        // Add items with frequency
        counterItems[0].forEach((k, v) -> trueFrequentItems.add(k));

        System.out.println("Number of true frequent items = " + trueFrequentItems.size());
        System.out.println("True Frequent Items: ");
        trueFrequentItems.sort(Comparator.comparingLong(Long::longValue)); // sort the list
        for(long item : trueFrequentItems) {
            System.out.println(item);
        }

        // Reservoir Sampling
        System.out.println("RESERVOIR SAMPLING");
        System.out.println("Size m of the sample = " + (int) Math.ceil(1 / phi));
        // Remove duplicates by converting to a Set and back to a List
        Set<Long> uniqueItemsSet = new HashSet<>(reservoirSampling[0]);
        ArrayList<Long> reservoirSamplingUnique = new ArrayList<>(uniqueItemsSet);
        System.out.println("Number of estimated frequent items = " + reservoirSamplingUnique.size());
        reservoirSamplingUnique.sort(Comparator.comparingLong(Long::longValue));
        System.out.println("Estimated frequent items:");
        for(Long item : reservoirSamplingUnique) {
            System.out.print(item + " ");
            if(trueFrequentItems.contains(item))
                System.out.println("+");
            else
                System.out.println("-");
        }

        // Sticky Sampling
        System.out.println("STICKY SAMPLING");
        System.out.println("Number of items in the Hash Table = " + stickySampling[0].size());
        // Remove items with frequency less than (phi - epsilon) * n
        stickySampling[0].entrySet().removeIf(entry -> entry.getValue() < (phi - epsilon) * n);
        System.out.println("Number of estimated frequent items = " + stickySampling[0].size());

        System.out.println("Estimated frequent items:");
        // Sort the items by key using an array list of tuples
        ArrayList<Long> stickySamplingSorted = new ArrayList<>();
        stickySampling[0].forEach((k, v) -> stickySamplingSorted.add(k));
        stickySamplingSorted.sort(Comparator.comparingLong(Long::longValue));

        for(Long item : stickySamplingSorted) {
            System.out.print(item + " ");
            if(trueFrequentItems.contains(item))
                System.out.println("+");
            else
                System.out.println("-");
        }

        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
    }

    /**
     * True Frequent Items Algorithm
     * @param batchItems- stream of items of the current batch
     * @return counterItems - map of counterItems
     */
    public static Hashtable<Long, Long> trueFrequentItems(List<Long> batchItems) {
        Hashtable<Long, Long> counterItems = new Hashtable<>();
        for(Long item: batchItems) {
            if(counterItems.containsKey(item))
                counterItems.replace(item, counterItems.get(item) + 1);
            else
                counterItems.put(item, 1L);
        }
        return counterItems;
    }

    /**
     * Reservoir Sampling Algorithm
     * @param batchItems - stream of items of current batch
     * @param reservoir - list of m-sampled items
     * @param phi - frequency threshold used to compute m
     * @param t - number of elements received until now
     */
    public static void reservoirSampling(List<Long> batchItems, ArrayList<Long> reservoir, float phi, long t) {
        Random random = new Random();
        int m = (int) Math.ceil(1 / phi);
        for(long item : batchItems) {
            if (reservoir.size() < m) {
                reservoir.add(item);
            } else {
                if (random.nextDouble() <= (double) m / t) {
                    int i = random.nextInt(m);
                    reservoir.set(i, item);
                }
            }
            t++;
        }
    }

    /**
     * epsilon-AFI with Sticky Sampling Algorithm
     * @param batchItems - batch of items
     * @param epsilon - accuracy parameter
     * @param delta - confidence parameter
     * @param phi - frequency threshold
     * @param n - length of the stream
     */
    public static void stickySampling(List<Long> batchItems, Hashtable<Long, Long> stickySampling, float epsilon, float delta, float phi, long n) {
        Random random = new Random();
        double r = Math.log(1 / (delta * phi)) / epsilon;
        for(long item : batchItems) {
            if (stickySampling.containsKey(item))
                stickySampling.replace(item, stickySampling.get(item) + 1);
            else
                if(random.nextDouble() <= r / n)
                    stickySampling.put(item, 1L);
        }
    }
}


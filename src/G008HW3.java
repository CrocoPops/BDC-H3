import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

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
        // Array of a JavaPairRDD<Long, Long> to store the stream of items


        AtomicReference<Hashtable<Long, Long>> counterItems = new AtomicReference<>(new Hashtable<>()); // Counter of the items for True Frequent Items
        AtomicReference<ArrayList<Long>> reservoirSampling = new AtomicReference<>(new ArrayList<>()); // Reservoir Sampling
        Hashtable<Long, Long> stickySampling = new Hashtable<>(); // epsilon-AFI with Sticky Sampling

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
                            stoppingSemaphore.release();
                            int offset = (int) (n - prevStreamLength);
                            batchItems = batchItems.subList(0, offset);
                        }

                        // Implementing the algorithms
                        counterItems.set(trueFrequentItems(batchItems, counterItems.get()));
                        reservoirSampling.set(reservoirSampling(batchItems, reservoirSampling.get(), phi, prevStreamLength));
                    }





                });

        // MANAGING STREAMING SPARK CONTEXT

        sc.start();
        stoppingSemaphore.acquire();

        // True Frequent Items
        System.out.println("Size of data structure to compute true frequent items = " + counterItems.get().size());
        List<Long> trueFrequentItems = new ArrayList<>();
        // Remove items with frequency less than phi * n
        counterItems.get().entrySet().removeIf(entry -> entry.getValue() < phi * n);
        // Add items with frequency
        counterItems.get().forEach((k, v) -> trueFrequentItems.add(k));

        System.out.println("Number of true frequent items = " + trueFrequentItems.size());
        System.out.println("True Frequent Items: ");
        trueFrequentItems.sort(Comparator.comparingLong(Long::longValue)); // sort the list
        for(long item : trueFrequentItems) {
            System.out.println(item);
        }

        // Reservoir Sampling
        System.out.println("Size m of the Reservoir sample = " + (int) Math.ceil(1 / phi));
        // Remove duplicates by converting to a Set and back to a List
        Set<Long> uniqueItemsSet = new HashSet<>(reservoirSampling.get());
        ArrayList<Long> reservoirSamplingUnique = new ArrayList<>(uniqueItemsSet);
        System.out.println("Number of estimated frequent items = " + reservoirSamplingUnique.size());
        reservoirSamplingUnique.sort(Comparator.comparingLong(Long::longValue));
        System.out.println("Reservoir Sampling:"); //TODO: Here I print the unique items. Check if the prof wants the unique ones or all
        for(Long item : reservoirSamplingUnique) {
            System.out.print(item + " ");
            if(trueFrequentItems.contains(item))
                System.out.println("+");
            else
                System.out.println("-");
        }

/*
        // Reservoir Sampling
        reservoirSampling.sort(Comparator.comparingLong(Long::longValue));
        System.out.println("Reservoir Sampling:");
        for(Long item : reservoirSampling) {
            System.out.println(item);
        }

        // epsilon-AFI computed with Sticky Sampling
        ArrayList<Tuple2<Long, Long>> l = new ArrayList<>();
        stickySampling.forEach((k, v) -> l.add(new Tuple2<>(k, v)));
        l.sort(Comparator.comparingLong(Tuple2::_2));
        System.out.println("Number of sticky sampling items = " + l.size());
        System.out.println("Sticky Sampling Items:");
        for(Tuple2<Long, Long> item : l)
            System.out.println(item._1());*/

        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
    }

    /**
     * True Frequent Items Algorithm
     * @param batchItems- stream of items of the current batch
     * @param counterItems - hashtable of items with their frequency
     * @return - hashtable of counterItems
     */
    public static Hashtable<Long, Long> trueFrequentItems(List<Long> batchItems, Hashtable<Long, Long> counterItems) {
        for(Long item: batchItems) {
            if(counterItems.contains(item))
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
     * @return - list of m-sampled items
     */
    public static ArrayList<Long> reservoirSampling(List<Long> batchItems, ArrayList<Long> reservoir,  float phi, long t) {
        int m = (int) Math.ceil(1 / phi);

        for(long item : batchItems) {
            if (reservoir.size() < m) {
                reservoir.add(item);
            } else {
                Random random = new Random();
                if (random.nextFloat() <= (float) m / t) {
                    int i = random.nextInt(m);
                    reservoir.set(i, item);
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


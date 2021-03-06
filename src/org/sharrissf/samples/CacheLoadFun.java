package org.sharrissf.samples;

import java.util.concurrent.BrokenBarrierException;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.TerracottaClientConfiguration;
import net.sf.ehcache.config.TerracottaConfiguration;

import org.terracotta.api.ClusteringToolkit;
import org.terracotta.api.TerracottaClient;
import org.terracotta.coordination.Barrier;
import org.terracotta.util.ClusteredAtomicLong;

/**
 * This is a small sample app that demonstrates the bulk loading characteristics of ehcache.
 * 
 * Usage: CacheLoad2Fun <nodeCount> <entryCount> <host> <port>
 * 
 * 
 * Only node count is required. entryCount defaults to 100k host defaults to "localhost" and port "9510"
 * 
 * By using the toolkit it is quite easy to have all the nodes coordinated to throw load at the same time.
 * 
 * ** Note that this test is written so that it can be rerun without restarting. What will happen is every entry will be replaced with a new
 * entry with the same key
 * 
 * *** IF YOU RUN THE TEST ALL ON ONE MACHINE YOU WILL LIKELY GET UNRELIABLE PERFORMANCE NUMBERS DO TO EITHER DISK, CPU, OR GC BOTTLENECKS
 * ***
 * 
 * @author steve
 * 
 */
public class CacheLoadFun {
    private final static int SLEEP_TIME = 1000;
    // private static final int MAX_ELEMENTS_ON_DISK = 1000000000;
    private final static int BATCH_SIZE = 5000;
    // private final static int TTL_IN_SECONDS = Integer.MAX_VALUE;

    private CacheManager cacheManager;

    private Ehcache cache;
    private Barrier coordinationBarrier;
    private ClusteredAtomicLong totalNodeCounter;
    private String nodeId;
    private final int entryCount;
    private final boolean doGets;
    private final int nodeCount;
    private int minValue = 0;

    /**
     * 
     * @param nodeCount
     *            - number of nodes to run. Each node will put entryCount number of elements into the cache
     * @param entryCount
     *            - number of entries to put in the cache
     * @param host
     *            - location of the server, defaults to localhost but for true performance tests do NOT share machines
     * @param port
     *            - listener port of the server
     * @param doGets
     * 
     * @throws InterruptedException
     * @throws BrokenBarrierException
     */
    public CacheLoadFun(int nodeCount, int entryCount, String host, String port, boolean doGets) throws InterruptedException,
            BrokenBarrierException {
        this.nodeCount = nodeCount;
        this.doGets = doGets;
        this.entryCount = entryCount;
        initializeCache(host, port);
        initialize(host, port);
    }

    /**
     * Kicks off the run of the test for this node
     * 
     * NOTE: It will wait for nodeCount nodes before actually running
     * 
     * @throws InterruptedException
     * @throws BrokenBarrierException
     */
    public void start() throws InterruptedException, BrokenBarrierException {
        resetNodeCounter();

        System.out.println("Waiting for all nodes to join... cache size is " + cache.getSize());

        waitForAllNodes();

        initializeMyNodeID();

        System.out.println("Starting... My NodeID: " + getNodeId());

        cache.setNodeCoherent(false);

        long t1 = System.currentTimeMillis();

        executeLoad(entryCount);

        cache.setNodeCoherent(true);
        cache.waitUntilClusterCoherent();

        System.out.println("Took: " + (System.currentTimeMillis() - t1) + " final size was " + cache.getSize());
    }

    private void executeLoad(final int entryCount) throws InterruptedException, BrokenBarrierException {
        long t = System.currentTimeMillis();
        byte[] value = buildValueString();
        for (int i = 0; i < entryCount; i++) {
            String k = createKeyFromCount(i);

            if ((i + 1) % BATCH_SIZE == 0) {

                value = buildValueString();
                if (doGets)
                    readOldEntries(i);
                if ((System.currentTimeMillis() - t) / 1000 > 5)
                    System.out.println("BIG");
                System.out.println(" i=" + (i + 1) + " time: " + (System.currentTimeMillis() - t) / 1000 + " key size: "
                        + k.getBytes().length);
                t = System.currentTimeMillis();
                try {
                    Thread.sleep(SLEEP_TIME);
                } catch (InterruptedException e) {

                }
            }

            cache.put(new Element(k, value));

        }

    }

    private String createKeyFromCount(int i) {
        return "K" + i + "-" + getNodeId();
    }

    private void readOldEntries(final int localEntryCount) {

        System.out.println("Starting read cycle at:" + minValue + "Local Entry count: " + localEntryCount);
        for (int j = minValue; j < localEntryCount; j++) {
            if (null == cache.get(createKeyFromCount(j))) {
                minValue = j;
            } else {
                System.out.println("Breaking at:" + j);
                return;
            }
            if ((j + 1) % BATCH_SIZE == 0) {
                System.out.println("Read 5000 minValue: " + minValue);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Ending read cycle at:" + minValue);
    }

    private byte[] buildValueString() {
        String baseString = "REPEAT: This is a test of the emergency broadcast system. Please stand by. Making the string a little bigger to oome faster";
        return (System.currentTimeMillis() + baseString).getBytes();
    }

    private void waitForAllNodes() throws InterruptedException, BrokenBarrierException {
        coordinationBarrier.await();
    }

    private void resetNodeCounter() {
        this.totalNodeCounter.set(0);
    }

    private void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    private String getNodeId() {
        return nodeId;
    }

    private void initializeMyNodeID() {
        this.setNodeId(totalNodeCounter.incrementAndGet() + "");

    }

    private void initialize(String host, String port) {
        ClusteringToolkit clustering = new TerracottaClient(host + ":" + port).getToolkit();

        coordinationBarrier = clustering.getBarrier("startBarrier", nodeCount);
        totalNodeCounter = clustering.getAtomicLong("startLong");
    }

    private void initializeCache(String host, String port) {
        Configuration cacheManagerConfig = new Configuration();

        // Add terracotta
        TerracottaClientConfiguration tcc = new TerracottaClientConfiguration();
        tcc.setUrl(host + ":" + port);
        cacheManagerConfig.addTerracottaConfig(tcc);

        // Add default cache
        cacheManagerConfig.addDefaultCache(new CacheConfiguration());

        // Create Cache
        CacheConfiguration cacheConfig = new CacheConfiguration("testCache", -1).maxElementsInMemory(2000).
        // maxElementsOnDisk(MAX_ELEMENTS_ON_DISK).timeToLiveSeconds(TTL_IN_SECONDS).
                eternal(true).terracotta(new TerracottaConfiguration().clustered(true).concurrency(2).storageStrategy("DCV2"));

        cacheManagerConfig.addCache(cacheConfig);

        this.cacheManager = new CacheManager(cacheManagerConfig);

        this.cache = this.cacheManager.getCache("testCache");
    }

    public static final void main(String[] args) throws InterruptedException, BrokenBarrierException {
        if (args.length < 1) {
            System.out.println("Invalid Arguments. Pass in number of nodes and optionally number entries, do gets, a host and a port");
            System.exit(1);
        }

        int nodeCount = Integer.parseInt(args[0]);
        if (nodeCount < 1) {
            System.out.println("Illegal node count: [" + args[0] + "]");
            System.exit(2);
        }

        int entryCount = args.length > 1 ? Integer.parseInt(args[1]) : 100000;
        boolean doGets = args.length > 2 ? Boolean.parseBoolean(args[2]) : false;
        String host = args.length > 3 ? args[3] : "localhost";
        String port = args.length > 4 ? args[4] : "9510";

        System.out.println("Starting " + nodeCount + " nodes each loading " + entryCount + " entries doing gets: " + doGets
                + " connecting to " + host + ":" + port);

        new CacheLoadFun(nodeCount, entryCount, host, port, doGets).start();
    }

    public int getNodeCount() {
        return nodeCount;
    }
}
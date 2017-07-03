package macrobase.analysis.stats.optimizer;

import com.google.common.base.Stopwatch;
import macrobase.analysis.stats.optimizer.util.*;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.Matrices;
import no.uib.cipr.matrix.QR;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class PCASkiingOptimizer extends SkiingOptimizer {

    private static final Logger log = LoggerFactory.getLogger(PCASkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;
    protected RealMatrix cachedTransform;

    protected PCA pca;
    protected PCAAlgo algo;


    public enum PCAAlgo{
        SVD, PI, TROPP, FAST
    }

    public enum work {
        REUSE, NOREUSE
    }

    public enum optimize{
        OPTIMIZE, NOOPTIMIZE
    }

    public PCASkiingOptimizer(double qThresh, int kExp, PCAAlgo algo, work reuseWork, optimize opt) {
        super(qThresh);
        this.KItersList = new HashMap<>();
        this.algo = algo;
        this.kScaling = kExp;

        switch(reuseWork) {
            case NOREUSE:
                this.reuse = false;
                break;
            default:
                this.reuse = true;
                break;
        }

        switch(opt) {
            case OPTIMIZE:
                this.opt = true;
                break;
            default:
                this.opt = false;
                break;
        }
    }

    public PCASkiingOptimizer(double qThresh, int kExp, PCAAlgo algo, work reuseWork, optimize opt, double fracReuse) {
        super(qThresh);
        this.KItersList = new HashMap<>();
        this.algo = algo;
        this.kScaling = kExp;
        this.reusePercent = fracReuse;

        switch(reuseWork) {
            case NOREUSE:
                this.reuse = false;
                break;
            default:
                this.reuse = true;
                break;
        }

        switch(opt) {
            case OPTIMIZE:
                this.opt = true;
                break;
            default:
                this.opt = false;
                break;
        }
    }

    public PCASkiingOptimizer(double qThresh, PCAAlgo algo) {
        super(qThresh);
        this.KItersList = new HashMap<>();
        this.algo = algo;
        this.reuse = true;
    }

    @Override
    public void preprocess(){
        super.preprocess();
        //use jni once to load
        DenseMatrix load = new DenseMatrix(3,3, new double[] {1,2,3,4,5,6,7,8,9},false);
        RealMatrix rLoad = new Array2DRowRealMatrix(Matrices.getArray(load));
        QR qr = new QR(3, 3);
        qr.factor(load);
        PCA tempPca = new PCASVD(rLoad);
        tempPca.transform(rLoad,1);
    }

    public void warmUp(int Nt){
        Random rand = new Random();
        int currTrain = trainList.size();
        for (int i = 0; i < Nt - currTrain; i++){
            int j = rand.nextInt(remList.size());
            trainList.add(remList.get(j));
        }
        RealMatrix trainMatrix = dataMatrix.getSubMatrix(ListtoPrimitive(trainList), allIndicesN);
        switch (algo){
            case PI:
                this.pca = new PCAPowerIteration(trainMatrix);
                break;

            case TROPP:
                this.pca = new PCATropp(trainMatrix);
                break;

            case FAST:
                this.pca = new PCAFast(trainMatrix);
                break;

            case SVD:
                this.pca = new PCASVD(trainMatrix);
                break;
        }
        trainList = new ArrayList<>();
    }

    public int[] ListtoPrimitive(List<Integer> in) {
        int[] out = new int[in.size()];
        for (int i = 0; i < in.size(); i++) {
            out[i] = in.get(i).intValue();
        }
        return out;
    }

    @Override
    public void fit(int Nt) {
        //temporary array list to sample without replacement, then reset for next run
        ArrayList<Integer> tempRemList = new ArrayList<>();
        for (int i: remList){
            tempRemList.add(i);
        }

        //get Nt points from train and move to test
        Random rand = new Random();
        trainList = new ArrayList<>();

        //importance sampling
        int currTrain = sampleList.size();
        for (int i : sampleList){
            trainList.add(i);
        }

        for (int i = 0; i < Nt - currTrain; i++){
                int j = rand.nextInt(tempRemList.size());
                trainList.add(tempRemList.get(j));
                tempRemList.remove(j);
        }
        RealMatrix trainMatrix = dataMatrix.getSubMatrix(ListtoPrimitive(trainList), allIndicesN);
        switch (algo){
            case PI:
                this.pca = new PCAPowerIteration(trainMatrix);
                break;

            case TROPP:
                this.pca = new PCATropp(trainMatrix);
                break;

            case FAST:
                this.pca = new PCAFast(trainMatrix);
                break;

            case SVD:
                this.pca = new PCASVD(trainMatrix);
                break;
        }
    }

    public void cacheInput(int high) {
        cachedTransform = this.pca.transform(dataMatrix, high);
    }

    public RealMatrix getCachedTransform(int K) {
        return cachedTransform.getSubMatrix(0, this.M - 1, 0, K - 1);
    }

    @Override
    public RealMatrix transform(int K) {
        return this.pca.transform(dataMatrix, K);
    }

    public RealMatrix getKFull(double targetLBR) {
        // computes the best K for a complete transformation using all the data
        double[] LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        int iters = 0;
        int low = 0;
        int high = Math.min(this.M, this.N) - 1;
        if (this.feasible) high = this.lastFeasible;
        int mid = (low + high) / 2;

        this.cacheInput(high);

        currTransform = this.getCachedTransform(high);
        LBR = evalK(targetLBR, currTransform);//this.meanLBR(iter, currTransform);
        if (targetLBR > LBR[2]) {
            return currTransform;
        }

        while (low < high) {
            currTransform = this.getCachedTransform(mid);
            LBR = evalK(targetLBR, currTransform);
            if (targetLBR < LBR[0]) {
                currTransform = this.getCachedTransform(mid - 1);
                LBR = evalK(targetLBR, currTransform);
                if (targetLBR > LBR[0]) {
                    this.feasible = true;
                    this.lastFeasible = mid;
                    return this.getCachedTransform(mid);
                }
                high = mid - 1;
            } else if (targetLBR > LBR[0]) {
                low = mid + 1;
            } else {
                high = mid;
            }
            iters += 1;
            mid = (low + high) / 2;
        }
        this.feasible = true;
        this.lastFeasible = mid;
        currKCI = LBR;
        return this.getCachedTransform(mid);
    }

    public void updateTrainWorkReuse(){
        if (reuse) {
            Double[] primLBRs = lastLBRs.toArray(new Double[lastLBRs.size()]);
            int numPoints = (int) Math.round(reusePercent*lastLBRs.size());
            int[] sortedLBRs = Arrays.copyOfRange(argSort(primLBRs, true),0,numPoints);
            for (int i: sortedLBRs){
                sampleList.add(lastIndicesA[i]);
                sampleList.add(lastIndicesB[i]);
                remList.remove((Object) lastIndicesA[i]);
                remList.remove((Object) lastIndicesB[i]);
            }
        }
    }

    public int getKCI(int currNt, double targetLBR) {
        //confidence interval based method for getting K
        double[] LBR;
        RealMatrix currTransform;

        int iters = 0;
        int low = 0;

        int high = Math.min(this.N, currNt) - 1;
        //if you weren't feasible, then the high remains. Else max is last feasible pt
        if (this.feasible) high = this.lastFeasible + 5; //TODO: arbitrary buffer room
        targetLBR += 0.002; //TODO: arbitrary buffer room
        int mid = (low + high) / 2;

        //If the max isn't feasible, just return it
        LBR = evalK(targetLBR, high);
        if (targetLBR > LBR[0]) {
            KItersList.put(currNt, ++iters);
            currKCI = LBR;
            updateTrainWorkReuse();
            return high;
        }


        //Binary search for lowest K that achieves LBR
        while (low != high) {
            LBR = evalK(targetLBR, mid);
            if (LBR[0] <= targetLBR) {
                low = mid + 1;
            } else {
                high = mid;
            }
            iters += 1;
            mid = (low + high) / 2;
        }
        this.feasible = true;
        this.lastFeasible = mid;
        KItersList.put(currNt, iters);
        currKCI = evalK(targetLBR,mid);
        updateTrainWorkReuse();
        return mid;
    }


    //same as getkci, just without the iter, becuase it's used for the baseline/full binary search w/ svd operation
    public int getKCI(double targetLBR) {
        //confidence interval based method for getting K
        double[] LBR;
        RealMatrix currTransform;

        int iters = 0;
        int low = 0;

        int high = Math.min(this.N, this.M) - 1;
        //if you weren't feasible, then the high remains. Else max is last feasible pt
        if (this.feasible) high = this.lastFeasible + 5; //TODO: arbitrary buffer room
        targetLBR += 0.002; //TODO: arbitrary buffer room
        int mid = (low + high) / 2;

        //If the max isn't feasible, just return it
        LBR = evalK(targetLBR, high);
        if (targetLBR > LBR[0]) {
            KItersList.put(this.M, ++iters);
            currKCI = LBR;
            updateTrainWorkReuse();
            return high;
        }


        //Binary search for lowest K that achieves LBR
        while (low != high) {
            LBR = evalK(targetLBR, mid);
            if (LBR[0] <= targetLBR) {
                low = mid + 1;
            } else {
                high = mid;
            }
            iters += 1;
            mid = (low + high) / 2;
        }
        this.feasible = true;
        this.lastFeasible = mid;
        KItersList.put(this.M, iters);
        currKCI = evalK(targetLBR,mid);
        updateTrainWorkReuse();
        return mid;
    }

    private double[] evalK(double LBRThresh, RealMatrix currTransform) {
        double[] CI = new double[]{0, 0, 0};
        double prevMean = 0;
        int numPairs = (this.M) * ((this.M) - 1) / 2;
        int currPairs = 100;//Math.max(5, this.M);//new Double(0.005*numPairs).intValue());
        while (currPairs < numPairs) {
            //log.debug("num pairs {}", currPairs);
            CI = this.LBRCI(currTransform, currPairs, qThresh);
            //all stopping conditions here
            if ((CI[0] > LBRThresh) || (CI[2] < LBRThresh) || (Math.abs(CI[1] - prevMean) < .01)) {
                return CI;//LBRThresh;
            } else {
                currPairs *= 2;
                prevMean = CI[1];
            }
        }
        return CI;
    }

    private double[] evalK(double LBRThresh, int K) {
        double[] CI = new double[]{0, 0, 0};
        double q = 1.96;
        double prevMean = 0;
        int numPairs = (this.M) * ((this.M) - 1) / 2;
        int currPairs = 100;//Math.max(5, this.M);//new Double(0.005*numPairs).intValue());
        while (currPairs < numPairs) {
            //log.debug("num pairs {}", currPairs);
            CI = this.LBRCI(K, currPairs, q);
            //all stopping conditions here:  LB > wanted; UB < wanted; mean didn't change much from last time
            if ((CI[0] > LBRThresh) || (CI[2] < LBRThresh) || (Math.abs(CI[1] - prevMean) < .001)) {
                return CI;//LBRThresh;
            } else {
                currPairs *= 2;
                prevMean = CI[1];
            }
        }
        return CI;
    }

    public double[] LBRCI(int K, int numPairs, double threshold) {
        int c = 0;
        int maxIndex = 0;

        int[] allIndices = this.allIndicesN;
        int[] indicesA = new int[numPairs];      // first point of pair
        int[] indicesB = new int[numPairs];      // second point of pair
        int[] tIndicesA = new int[numPairs];     // indices to pull from
        int[] tIndicesB = new int[numPairs];     //
        int[] jointIndices;
        int[] jointIndexMapping;
        int[] kIndices;

        Set<Integer> jIndices = new HashSet<>(); //set of all datapoints needed
        Random rand = new Random();

        RealMatrix transformedData;
        RealVector transformedDists;
        RealVector trueDists;

        List<Double> LBRs;
        double mean = 0;
        double std = 0;
        double slop;

        kIndices = Arrays.copyOf(allIndices, K); //list to get up to k

        // No train and test separation because it isn't really required, and you'll run out of points towards the end
        for (int i = 0; i < numPairs; i++) {
            indicesA[i] = rand.nextInt(M);//remList.get(rand.nextInt(remList.size()));
            indicesB[i] = rand.nextInt(M);//remList.get(rand.nextInt(remList.size()));
            while (indicesA[i] == indicesB[i]) {
                indicesA[i] = rand.nextInt(M);//remList.get(rand.nextInt(remList.size()));
            }
            //calculating indices union of A and B and the max index
            jIndices.add(indicesA[i]);
            jIndices.add(indicesB[i]);
            if (Math.max(indicesA[i], indicesB[i]) > maxIndex) {
                maxIndex = Math.max(indicesA[i], indicesB[i]);
            }
        }

        //computing a mapping between these indices and indexA/B
        jointIndices = new int[jIndices.size()];
        jointIndexMapping = new int[maxIndex + 1]; //TODO: if this becomes too big, move to hashmap to save on space lol
        for (Object val : jIndices.toArray()) {
            jointIndexMapping[((Integer) val).intValue()] = c; //mapping from original index to new index
            jointIndices[c++] = ((Integer) val).intValue(); //mapping from new index to original index; basically primitive version of jIndices
        }

        for (int i = 0; i < numPairs; i++) {
            tIndicesA[i] = jointIndexMapping[indicesA[i]];
            tIndicesB[i] = jointIndexMapping[indicesB[i]];
        }

        //get and transform a single matrix with only indices that are used
        transformedData = dataMatrix.getSubMatrix(jointIndices, allIndices);
        transformedData = this.pca.transform(transformedData, K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(tIndicesA, kIndices), transformedData.getSubMatrix(tIndicesB, kIndices)).mapMultiply(1.0);
        trueDists = this.calcDistances(this.dataMatrix.getSubMatrix(indicesA, allIndices), this.dataMatrix.getSubMatrix(indicesB, allIndices));
        LBRs = this.calcLBRList(trueDists, transformedDists);

        //storing last values for work reuse
        lastLBRs = LBRs;
        lastIndicesA = indicesA;
        lastIndicesB = indicesB;

        for (double l : LBRs) {
            mean += l;
        }
        mean /= numPairs;

        for (double l : LBRs) {
            std += (l - mean) * (l - mean);
        }
        std = Math.sqrt(std / numPairs);
        slop = (threshold * std) / Math.sqrt(numPairs);
        return new double[]{mean - slop, mean, mean + slop, std * std};
    }

    public Map<String,Map<Integer, Double>> computeLBRs(){
        //confidence interval based method for getting K
        Map<Integer, Double> LBRs = new HashMap<>();
        Map<Integer, Double> times = new HashMap<>();
        Map<String, Map<Integer, Double>> results = new HashMap<>();

        Stopwatch sw =  Stopwatch.createUnstarted();

        sw.start();
        this.fit(M);
        int max = Math.min(N, M);
        this.cacheInput(max);

        sw.stop();
        times.put(0, (double) sw.elapsed(TimeUnit.MILLISECONDS));

        double[] CI;
        int interval = Math.max(2, Math.min(N, M)/30);
        RealMatrix currTransform;
        for (int i = 2; i <= max; i += interval) {
            sw.reset();
            sw.start();
            currTransform = this.getCachedTransform(i);
            sw.stop();

            CI = this.LBRCI(currTransform, M, qThresh);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1], CI[2]);
            LBRs.put(i, CI[1]);
            times.put(i, (double) sw.elapsed(TimeUnit.MILLISECONDS));
        }
        results.put("LBR", LBRs);
        results.put("time", times);
        return results;
    }

    public long[] getFullSVD(double lbr){
        return getFullSVD(lbr, 1.0);
    }

    public long[] getFullSVD(double lbr, double propn){
        long[] ktime = new long[] {0,0};
        int k;
        int samples = new Double(M*propn).intValue();
        Stopwatch sw = Stopwatch.createUnstarted();

        sw.start();
        fit(samples);
        k = getKCI(samples,lbr);
        sw.stop();

        ktime[0] = k;
        ktime[1] = sw.elapsed(TimeUnit.MILLISECONDS);
        return ktime;
    }

    public Map getKItersList() {
        return KItersList;
    }
}


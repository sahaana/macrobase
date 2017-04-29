package macrobase.analysis.stats.optimizer;

import macrobase.analysis.stats.optimizer.util.*;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//import org.apache.commons.math3.distribution.MultivariateNormalDistribution;

public class PCASkiingOptimizer extends SkiingOptimizer {

    private static final Logger log = LoggerFactory.getLogger(PCASkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;
    protected RealMatrix cachedTransform;

    protected PCA pca;
    protected PCAAlgo algo;

    public enum PCAAlgo{
        SVD, PI, TROPP, FAST;
    }


    public PCASkiingOptimizer(double epsilon, PCAAlgo algo) {
        super(epsilon);
        this.KItersList = new HashMap<>();
        this.algo = algo;

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
        //get Nt points from train and move to test
        Random rand = new Random();
        int currTrain = trainList.size();
        for (int i = 0; i < Nt - currTrain; i++){
            int j = rand.nextInt(testList.size());
            trainList.add(testList.get(j));
            testList.remove(j);
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
        int iter = 8008;
        int low = 0;
        int high = Math.min(this.M, this.Nproc) - 1;
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
            LBR = evalK(targetLBR, currTransform);//this.meanLBR(iter, currTransform);
            if (targetLBR < LBR[0]) {
                currTransform = this.getCachedTransform(mid - 1);
                LBR = evalK(targetLBR, currTransform);//this.meanLBR(iter, currTransform);
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

    //This is deprecated, but hasn't been cut yet
    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        //simply uses the mean LBR to do K computations with
        double LBR;
        RealMatrix currTransform;

        int iters = 0;
        int low = 0;
        int high = Math.min(this.Nproc, this.NtList.get(iter)) - 1;
        if (this.feasible) high = this.lastFeasible;
        int mid = (low + high) / 2;

        //System.out.println(this.M);
        currTransform = this.transform(high);
        LBR = this.meanLBR(iter, currTransform);
        if (targetLBR > LBR) {
            KItersList.put(this.NtList.get(iter), ++iters);
            return currTransform;
        }

        while (low < high) {
            currTransform = this.transform(mid);
            LBR = this.meanLBR(iter, currTransform);
            if (targetLBR < LBR) {
                currTransform = this.transform(mid - 1);
                LBR = this.meanLBR(iter, currTransform);
                if (targetLBR > LBR) {
                    this.feasible = true;
                    this.lastFeasible = mid;
                    KItersList.put(this.NtList.get(iter), iters);
                    return this.getCachedTransform(mid);
                }
                high = mid - 1;
            } else if (targetLBR > LBR) {
                low = mid + 1;
            } else {
                high = mid;
                //KItersList.put(this.NtList.get(iter), iters);
                //return currTransform;
            }
            iters += 1;
            mid = (low + high) / 2;
        }
        this.feasible = true;
        this.lastFeasible = mid;
        KItersList.put(this.NtList.get(iter), iters);
        return this.transform(mid);
    }


    public RealMatrix getKCICached(int iter, double targetLBR) {
        //confidence interval based method for getting K
        double[] LBR;
        RealMatrix currTransform;

        int iters = 0;
        int low = 0;

        int high = Math.min(this.N, this.NtList.get(iter)) - 1;
        if (this.feasible) high = this.lastFeasible + 5; //TODO: arbitrary buffer room
        targetLBR += 0.002; //TODO: arbitrary buffer room
        int mid = (low + high) / 2;

        this.cacheInput(high);

        currTransform = this.getCachedTransform(high);
        LBR = evalK(targetLBR, currTransform);
        if (targetLBR > LBR[0]) {
            KItersList.put(this.NtList.get(iter), ++iters);
            currKCI = LBR;
            return currTransform;
        }

        while (low != high) {
            currTransform = this.getCachedTransform(mid);
            LBR = evalK(targetLBR, currTransform);
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
        KItersList.put(this.NtList.get(iter), iters);
        currKCI = LBR;
        return this.getCachedTransform(mid);
    }

    public void updateTrainWorkReuse(){
        Double[] primLBRs = lastLBRs.toArray(new Double[lastLBRs.size()]);
        int numPoints = (int) Math.round(reusePercent*lastLBRs.size());
        int[] sortedLBRs = Arrays.copyOfRange(argSort(primLBRs, true),0,numPoints);
        for (int i: sortedLBRs){
            trainList.add(lastIndicesA[i]);
            trainList.add(lastIndicesB[i]);
            testList.remove((Object) lastIndicesA[i]);
            testList.remove((Object) lastIndicesB[i]);
        }
    }

    public RealMatrix getKCI(int iter, double targetLBR) {
        //confidence interval based method for getting K
        double[] LBR;
        RealMatrix currTransform;

        int iters = 0;
        int low = 0;

        int high = Math.min(this.N, this.NtList.get(iter)) - 1;
        //if you weren't feasible, then the high remains. Else max is last feasible pt
        if (this.feasible) high = this.lastFeasible + 5; //TODO: arbitrary buffer room
        targetLBR += 0.002; //TODO: arbitrary buffer room
        int mid = (low + high) / 2;

        //If the max isn't feasible, just return it
        LBR = evalK(targetLBR, high);
        if (targetLBR > LBR[0]) {
            KItersList.put(this.NtList.get(iter), ++iters);
            currKCI = LBR;
            currTransform = this.transform(high);
            updateTrainWorkReuse();
            return currTransform;
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
        KItersList.put(this.NtList.get(iter), iters);
        currKCI = LBR;
        updateTrainWorkReuse();
        return this.transform(mid);
    }

    private double[] evalK(double LBRThresh, RealMatrix currTransform) {
        double[] CI = new double[]{0, 0, 0};
        double q = 1.96;
        double prevMean = 0;
        int numPairs = (this.M) * ((this.M) - 1) / 2;
        int currPairs = 100;//Math.max(5, this.M);//new Double(0.005*numPairs).intValue());
        while (currPairs < numPairs) {
            //log.debug("num pairs {}", currPairs);
            CI = this.LBRCI(currTransform, currPairs, q);
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

        // Generate list to get entire dimension
        //for (int i = 0; i < N; i++) {
        //    allIndices[i] = i;
        //}
        kIndices = Arrays.copyOf(allIndices, K); //list to get up to k

        for (int i = 0; i < numPairs; i++) {
            indicesA[i] = testList.get(rand.nextInt(testList.size()));
            indicesB[i] = testList.get(rand.nextInt(testList.size()));
            while (indicesA[i] == indicesB[i]) {
                indicesA[i] = testList.get(rand.nextInt(testList.size()));
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
        transformedData = rawDataMatrix.getSubMatrix(jointIndices, allIndices);
        transformedData = this.pca.transform(transformedData, K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(tIndicesA, kIndices), transformedData.getSubMatrix(tIndicesB, kIndices)).mapMultiply(Math.sqrt(this.N) / Math.sqrt(this.Nproc));
        trueDists = this.calcDistances(this.rawDataMatrix.getSubMatrix(indicesA, allIndices), this.rawDataMatrix.getSubMatrix(indicesB, allIndices));
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


    public Map<Integer, Double> computeLBRs() {
        //confidence interval based method for getting K
        Map<Integer, Double> LBRs = new HashMap<>();
        double[] CI = {0, 0, 0};
        int interval = Math.max(2, this.N / 32);
        RealMatrix currTransform;
        for (int i = 2; ((i <= this.N) && (CI[1] <= .9999)); i += interval) {
            currTransform = this.transform(i);
            CI = this.LBRCI(currTransform, M, 1.96);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1], CI[2]);
            LBRs.put(i, CI[1]);
        }
        return LBRs;
    }

    public Map getKItersList() {
        return KItersList;
    }

    public RealMatrix getTransformation() {
        return this.pca.getTransformationMatrix();
    }
}


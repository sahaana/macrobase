package macrobase.analysis.stats.optimizer;


import macrobase.analysis.stats.optimizer.util.PCA;
import macrobase.datamodel.Datum;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class SkiingOptimizer {
    private static final Logger log = LoggerFactory.getLogger(SkiingOptimizer.class);
    protected int N; //original data dimension
    protected int Nproc; //processed data dimension (PAA, Mary, etc)
    protected int M; //original number of training samples

    protected int[] kDiffs;
    protected int prevK;
    protected double[] currKCI;
    protected int numDiffs;
    protected int NtInterval;

    protected List<Integer> NtList;
    protected Map<Integer, Integer> KList;
    protected Map<Integer, double[]> LBRList;
    protected Map<Integer, Double> trainTimeList;
    protected Map<Integer, Double> predictedTrainTimeList;
    protected Map<Integer, Integer> kPredList;


    protected double epsilon;
    protected int s;
    protected int b;

    protected RealMatrix rawDataMatrix;
    protected RealMatrix dataMatrix;

    protected boolean feasible;
    protected int lastFeasible;

    protected int Ntdegree;
    protected int kScaling;
    protected PolynomialCurveFitter fitter;
    protected WeightedObservedPoints MDruntimes;

    public SkiingOptimizer(double epsilon, int b, int s){
        this.numDiffs = 3;
        this.epsilon = epsilon;
        this.s = s;
        this.b = b;

        this.NtList = new ArrayList<>();
        this.LBRList = new HashMap<>();
        this.KList = new HashMap<>();
        this.trainTimeList = new HashMap<>();
        this.predictedTrainTimeList = new HashMap<>();
        this.kPredList = new HashMap<>();
        this.kDiffs = new int[this.numDiffs]; //TODO: 3 to change to general param

        this.prevK = 0;
        this.currKCI = new double[] {0, 0, 0};

        this.feasible = false;
        this.lastFeasible = 0;

        this.kScaling = 1;
        this.Ntdegree = 2;
        this.MDruntimes = new WeightedObservedPoints();
        MDruntimes.add(0,0);
        this.fitter = PolynomialCurveFitter.create(Ntdegree);
    }

    public void extractData(List<Datum> records){
        ArrayList<double[]> metrics = new ArrayList<>();
        for (Datum d: records) {
            metrics.add(d.metrics().toArray());
        }
        this.M = metrics.size();
        this.N = metrics.get(0).length;

        double[][] metricArray = new double[M][];
        for (int i = 0; i < M; i++){
            metricArray[i] = metrics.get(i);
        }
        this.rawDataMatrix = new Array2DRowRealMatrix(metricArray);
        //RealMatrix cov = new Covariance(this.rawDataMatrix).getCovarianceMatrix();

        this.NtInterval = 10; //Math.max(3, new Double(this.M*0.01).intValue()); //arbitrary 1%
    }

    public void shuffleData(){
        List<Integer> indicesM = new ArrayList<>();
        int[] indicesN = new int[N];
        for (int i = 0; i < N; i++){
            indicesN[i] = i;
        }
        for (int i = 0; i < M; i++){
            indicesM.add(i); //TODO: this is stupid
        }
        Collections.shuffle(indicesM);
        int[] iA = ArrayUtils.toPrimitive(indicesM.toArray(new Integer[M]));

        rawDataMatrix = rawDataMatrix.getSubMatrix(iA, indicesN);
    }

    public void preprocess(){
        Nproc = N;
        dataMatrix = rawDataMatrix;
    }

    public int getNextNtFromList(int iter, int currNt, int maxNt){
        //int[] Nts = {11,12,13,14,15,16,17,18,19,20,21,22,23,25,30,35,40,45,50,55,60, 65,70,80,90,100,110,125,150,175,200,300,400,500,600};
        //if (iter == 0){ return 0; }
        //int K =  KList.get(currNt);
        int[] Nts = {10, 20,30,40,50,60,70,80,90,100,110,125,150,175,200};
        if (iter >= Nts.length || NtList.size() >= maxNt) {
        //    NtList.add(2000000);
            return 2000000;
        }
        //NtList.add(Nts[iter]);
        return Nts[iter];
    }

    public int getNextNtIncreaseOnly(int iter, int currNt, int maxNt){
        double avgDiff = 0;

        if (iter == 0) {
         //   NtList.add(NtInterval);
            return NtInterval;
        }

        for (double i: kDiffs){
            avgDiff += i/numDiffs;
        }

        //double the interval if it's choking
        if (avgDiff == NtInterval){
            NtInterval = NtInterval*2;
        }

        //THIS IS FOR TESTING WITHOUT FULL MIC DROP FUNCTIONALITY
        if (currNt >= 1100){
        //    NtList.add(M+1);
            return M+1;
        }

        //NtList.add(NtInterval + currNt);
        return NtInterval+currNt;
    }

    public int getNextNtBasicDoubling(int iter, int currNt, int maxNt){
        double avgDiff = 0;

        if (iter == 0) {
        //    NtList.add(NtInterval);
            return NtInterval;
        }

        for (double i: kDiffs){
            avgDiff += i/numDiffs;
        }

        //if things haven't changed much on average, you can stop
        if (avgDiff <= 1){
        //    NtList.add(M+1);
            return M+1;
        }

        //double the interval if it's choking and is increasing by the interval each time
        if (avgDiff == NtInterval){
            NtInterval = NtInterval*2;
        }

        //halve the interval if it's aight. overshoots tho. lbr-based?
        // if (LBRList.get(currNt) >= )
        if (avgDiff < NtInterval/2){
            NtInterval = new Double(NtInterval/2).intValue();
        }

        //NtList.add(NtInterval + currNt);
        return NtInterval+currNt;
    }

    public int getNextNt(int iter, int currNt, int maxNt){
        int nextNt =  getNextNtIncreaseOnly(iter, currNt, maxNt);
        NtList.add(nextNt);
        return nextNt;
    }

    public int getNextNtObjectiveFunc(int iter, int currNt, int maxNt){
        double prevObjective = Math.pow(KList.get(currNt), kScaling) + trainTimeList.get(currNt);
        double kTimeGuess = Math.pow(this.kPredList.get(currNt),kScaling);
        double[] MDtimeCoeffs = fitter.fit(this.MDruntimes.toList());
        double NtTimeGuess = 0;
        int nextNt = getNextNtIncreaseOnly(iter, currNt, maxNt); //getNextNtBasicDoubling(iter,currNt,maxNt);
        double currObjective;

        for (int i = 0; i <= this.Ntdegree; i++){
            NtTimeGuess += MDtimeCoeffs[i]*Math.pow(nextNt, i);
        }
        this.predictedTrainTimeList.put(nextNt, NtTimeGuess);
        currObjective = NtTimeGuess + kTimeGuess;

        if (currObjective <= prevObjective){//nextNt <= 1000){
            return nextNt;
        }
        return M+1;
    }

    public int getNextNtPE(int iter, int currNt, int maxNt, boolean attainedLBR){
        int nextNt;
        if (!attainedLBR){
            nextNt = getNextNtIncreaseOnly(iter, currNt, maxNt);
            NtList.add(nextNt);
            return nextNt;
        }
        nextNt = getNextNtObjectiveFunc(iter, currNt, maxNt);
        log.debug("NextNt {}", nextNt);
        NtList.add(nextNt);
        return nextNt;
    }

    //predicting K for the "next" iteration and Nt
    public void predictK(int iter, int currNt){
        if (iter == 1){
            int guess = kDiffs[(iter-1) % numDiffs] + prevK;
            this.kPredList.put(currNt, guess);
            return;
        }
        double ratio = kDiffs[(iter-1) % numDiffs]/ (NtList.get(iter-1) - NtList.get(iter-2));
        int guess = (int) Math.round(ratio*(currNt - NtList.get(iter-1)));
        this.kPredList.put(currNt, guess + prevK);
        return;
    }

    public void updateMDRuntime(int currNt, double MDtime){
        MDruntimes.add(currNt, MDtime);
        trainTimeList.put(currNt, MDtime);
    }


    public double meanLBR(int iter, RealMatrix transformedData){
        int numPairs = M;
        int K = transformedData.getColumnDimension();
        //int currNt = NtList.get(iter);

        int[] allIndices = new int[this.N];
        int[] indicesA = new int[numPairs];
        int[] indicesB = new int[numPairs];
        int[] kIndices;

        Random rand = new Random();

        RealVector transformedDists;
        RealVector trueDists;

        for (int i = 0; i < numPairs; i++){
            indicesA[i] = rand.nextInt(M);// - currNt) + currNt;
            indicesB[i] = rand.nextInt(M );//- currNt) + currNt;
            while(indicesA[i] == indicesB[i]){
                indicesA[i] = rand.nextInt(M);// - currNt) + currNt;
            }
        }

        for (int i = 0; i < N; i++){
            allIndices[i] = i; //TODO: // FIXME: 9/2/16
        }
        kIndices = Arrays.copyOf(allIndices,K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(Math.sqrt(this.N/this.Nproc));
        trueDists = this.calcDistances(this.rawDataMatrix.getSubMatrix(indicesA,allIndices), this.rawDataMatrix.getSubMatrix(indicesB,allIndices));
        return this.LBR(trueDists, transformedDists);
    }


    public double[] LBRCI(RealMatrix transformedData, int numPairs, double threshold){
        int K = transformedData.getColumnDimension();
        //int currNt = NtList.get(iter);

        int[] allIndices = new int[this.N];
        int[] indicesA = new int[numPairs];
        int[] indicesB = new int[numPairs];
        int[] kIndices;

        Random rand = new Random();

        RealVector transformedDists;
        RealVector trueDists;

        List<Double> LBRs;
        double mean = 0;
        double std = 0;
        double slop;

        for (int i = 0; i < numPairs; i++){
            indicesA[i] = rand.nextInt(M);// - currNt) + currNt;
            indicesB[i] = rand.nextInt(M );//- currNt) + currNt;
            while(indicesA[i] == indicesB[i]){
                indicesA[i] = rand.nextInt(M);// - currNt) + currNt;
            }
        }

        for (int i = 0; i < N; i++){
            allIndices[i] = i; //TODO: FIXME: 9/2/16
        }
        kIndices = Arrays.copyOf(allIndices,K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(Math.sqrt(this.N)/Math.sqrt(this.Nproc));
        trueDists = this.calcDistances(this.rawDataMatrix.getSubMatrix(indicesA,allIndices), this.rawDataMatrix.getSubMatrix(indicesB,allIndices));
        LBRs = this.calcLBRList(trueDists, transformedDists);
        for(double l: LBRs){
            mean += l;
        }
        mean /= numPairs;

        for(double l: LBRs){
            std += (l - mean)*(l - mean);
        }
        std = Math.sqrt(std/numPairs);
        slop = (threshold*std)/Math.sqrt(numPairs);
        return new double[] {mean-slop, mean, mean+slop, std*std};
    }






    //TODO: this should really just call calcLBRList
    public double LBR(RealVector trueDists, RealVector transformedDists){
        int num_entries = trueDists.getDimension();
        double lbr = 0;
        for (int i = 0; i < num_entries; i++) {
            if (transformedDists.getEntry(i) == 0){
                if (trueDists.getEntry(i) == 0) lbr += 1; //they were same to begin w/, so max of 1
                else lbr += 0; //can never be negative, so lowest
            }
            else lbr += transformedDists.getEntry(i)/trueDists.getEntry(i);
        }

        //arbitrarily choose to average all of the LBRs
        return lbr/num_entries;
    }

    public List<Double> calcLBRList(RealVector trueDists, RealVector transformedDists){
        int num_entries = trueDists.getDimension();
        List<Double> lbr = new ArrayList<>();
        for (int i = 0; i < num_entries; i++) {
            if (transformedDists.getEntry(i) == 0){
                if (trueDists.getEntry(i) == 0) lbr.add(1.0); //they were same to begin w/, so max of 1
                else lbr.add(0.0); //can never be negative, so lowest
            }
            else lbr.add(transformedDists.getEntry(i)/trueDists.getEntry(i));
        }
        return lbr;
    }

    public int getNproc(){return Nproc;}

    public int getN(){ return N;}

    public int getM(){return M;}

    public RealMatrix getDataMatrix() {return dataMatrix;}

    public void setKDiff(int iter, int currK){
        kDiffs[iter % numDiffs] = currK - prevK;
        prevK = currK;
    }

    public void addNtList(int Nt){ NtList.add(Nt); }

    public void setKList(int k, int v){ KList.put(k,v); }

    public void setLBRList(int k, double[] v){
        LBRList.put(k, v);
    }

    public void setTrainTimeList(int k, double v){
        trainTimeList.put(k, v);
    }

    public double[] getCurrKCI(){ return currKCI; }

    public int getNtList(int iter){ return NtList.get(iter); }

    public Map getLBRList(){ return LBRList; }

    public Map getTrainTimeList(){ return trainTimeList; }

    public Map getPredictedTrainTimeList(){ return predictedTrainTimeList; }

    public Map getKList(){ return KList; }

    public Map getKPredList(){ return kPredList; }
    
    public abstract void fit(int Nt);

    public abstract RealMatrix transform(int K);

    public abstract RealMatrix getK(int iter, double targetLBR);

   //TODO: rest are util funcs that should probably just be moved

    public RealVector calcDistances(RealMatrix dataA, RealMatrix dataB){
        int rows = dataA.getRowDimension();
        RealMatrix differences = dataA.subtract(dataB);
        RealVector distances = new ArrayRealVector(rows);
        RealVector currVec;
        for (int i = 0; i < rows; i++){
            currVec = differences.getRowVector(i);
            distances.setEntry(i, currVec.getNorm());
        }
        return distances;
    }

}

package macrobase.analysis.stats.optimizer;


import macrobase.datamodel.Datum;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class SkiingOptimizer {
    private static final Logger log = LoggerFactory.getLogger(SkiingOptimizer.class);
    protected int N; //original data dimension
    protected int M; //original number of training samples
    protected List<Integer> trainList;
    protected List<Integer> remList;
    protected List<Integer> sampleList;

    protected int kDiff;
    protected double MDDiff;
    protected int prevK;
    protected double prevMDTime;
    protected double[] currKCI;
    protected int numDiffs;
    protected int NtInterval;
    protected double[] NtSchedule;

    protected List<Integer> NtList;
    protected Map<Integer, Integer> KList;
    protected Map<Integer, double[]> LBRList;
    protected Map<Integer, Double> trainTimeList;
    protected Map<Integer, Double> predictedTrainTimeList;
    protected Map<Integer, Integer> kPredList;
    protected Map<Integer, Double> trueObjective;
    protected Map<Integer, Double> predictedObjective;
    protected Map<Integer, Double> fObjective;
    protected Map<Integer, Double> dObjective;

    double dropTimeSoFar;


    protected double qThresh;

    protected RealMatrix dataMatrix;

    protected boolean feasible;
    protected boolean firstKDrop;
    protected int lastFeasible;

    protected int Ntdegree;
    protected int kScaling;
    protected PolynomialCurveFitter fitter;
    protected WeightedObservedPoints MDruntimes;

    //general indices that go 0 to M/N
    protected int[] allIndicesN;
    protected int[] allIndicesM;

    //indices for maintaining lists of last LBR, and corresponding indices in pairs
    protected double reusePercent;
    protected int[] lastIndicesA;
    protected int[] lastIndicesB;
    protected List<Double> lastLBRs;


    protected boolean opt;
    protected boolean reuse;


    public SkiingOptimizer(double qThresh) {
        this.numDiffs = 3; //TODO: 3 to change to general param
        this.qThresh = qThresh;

        this.NtList = new ArrayList<>();
        this.LBRList = new HashMap<>();
        this.KList = new HashMap<>();
        this.trainTimeList = new HashMap<>();
        this.predictedTrainTimeList = new HashMap<>();
        this.trueObjective = new HashMap<>();
        this.predictedObjective = new HashMap<>();
        this.fObjective = new HashMap<>();
        this.dObjective = new HashMap<>();
        this.kPredList = new HashMap<>();

        this.MDDiff = 0;
        this.kDiff = 0;
        this.prevK = 0;
        this.prevMDTime = 0;
        this.currKCI = new double[]{0, 0, 0};
        this.dropTimeSoFar = 0;

        this.feasible = false;
        this.firstKDrop = true;
        this.lastFeasible = 0;

        this.kScaling = 3;
        this.Ntdegree = 2;
        this.MDruntimes = new WeightedObservedPoints();
        MDruntimes.add(0, 0);
        this.fitter = PolynomialCurveFitter.create(Ntdegree);

        this.reusePercent = .025;

        this.reuse = true;
        this.opt = false;
    }

    public void extractData(List<Datum> records){
        ArrayList<double[]> metrics = new ArrayList<>();
        for (Datum d: records) {
            metrics.add(d.metrics().toArray());
        }
        this.M = metrics.size();
        this.N = metrics.get(0).length;


        this.trainList = new ArrayList<>();
        this.remList = new ArrayList<>();
        this.sampleList = new ArrayList<>();
        this.allIndicesN = new int[N];
        this.allIndicesM = new int[M];
        double[][] metricArray = new double[M][];

        for (int i = 0; i < M; i++){
            metricArray[i] = metrics.get(i);
            this.remList.add(i, i);
            this.allIndicesM[i] = i;
        }
        for (int i = 0; i < N; i++){
            this.allIndicesN[i] = i;
        }

        this.dataMatrix = new Array2DRowRealMatrix(metricArray);
    }

    public void extractData(List<Datum> records, Boolean limit){
        ArrayList<double[]> metrics = new ArrayList<>();
        for (Datum d: records) {
            metrics.add(d.metrics().toArray());
        }

        this.M = metrics.size();

        if (limit){
            int Ntemp = metrics.get(0).length;
            this.N = (Ntemp/20) * 20;

        }
        else {
            this.N = metrics.get(0).length;
        }

        this.trainList = new ArrayList<>();
        this.remList = new ArrayList<>();
        this.sampleList = new ArrayList<>();
        this.allIndicesN = new int[N];
        this.allIndicesM = new int[M];
        double[][] metricArray = new double[M][];

        for (int i = 0; i < M; i++){
            metricArray[i] = Arrays.copyOf(metrics.get(i),N);
            this.remList.add(i, i);
            this.allIndicesM[i] = i;
        }
        for (int i = 0; i < N; i++){
            this.allIndicesN[i] = i;
        }

        this.dataMatrix = new Array2DRowRealMatrix(metricArray);
    }

    public void preprocess(){
        this.NtInterval = Math.max(10, new Double(this.M*0.05).intValue()); //arbitrary 5%
        this.NtSchedule = new double[] {0.01, 0.02, 0.03, 0.04, 0.05, 0.10, 0.20, .30, .65, 1.5};//, 1.0, 1.5};

        ///uncomment for increasing data
        this.NtInterval = 500;
        this.NtInterval = M;

        //touch all of the data
        double touch = 0;
        for (int i = 0; i < M; i++){
            for (int j = 0; j < N; j++){
                touch += this.dataMatrix.getEntry(i,j);
            }
        }
    }

    //use this for increasing data
    public int getNextNtFixedInterval(int iter, int currNt){
        if (iter == 0) {
            return NtInterval;
        }
        return NtInterval + currNt;
    }

    public int getNextNtFixedSchedule(int iter, int currNt){
        Double Nt =  NtSchedule[iter] * M;
        return Nt.intValue();
    }

    public Map<Integer, double[]> bundleMDTimeGuess(){
        Map<Integer, double[]> predVact = new HashMap<>();
        for (int Nt: trainTimeList.keySet()){
            predVact.put(Nt, new double[] {trainTimeList.get(Nt), predictedTrainTimeList.getOrDefault(Nt,0.0)});
        }
        return predVact;
    }

    // this is always called before anything else happens that iter
    public int getNextNtPE(int iter, int currNt){
        // tentative next Nt
        ///int nextNt = getNextNtFixedSchedule(iter, currNt);
        int nextNt = getNextNtFixedInterval(iter, currNt);
        //iter 0 is special because currNt has not been run yet, so no #s exist
        if (iter == 0){
            NtList.add(nextNt);
            return nextNt;
        }

        //for increasing data
        //if (nextNt > 2000) {
        //    return 10000000;
        //}

        if (nextNt > M){
            return nextNt;
        }

        //for all other iters, MD has been run with currNt
        //if (opt){
        nextNt = getNextNtObjective(iter, currNt, nextNt);
        //}
        NtList.add(nextNt);
        return nextNt;
    }

    public double kOrigFunc(int k){
        return Math.max(0.0, (M * Math.pow(k, kScaling)));
    }

    public double kFunc(int k){
        //      a        + b*x[0]  + c*x[1]    + d*x[0]*x[1] + e*x[0]*x[0] + f*x[0]*x[0]*x[1] 0=M,1=D
        // V1
        //[-5.63773557  0.01784894 -0.20522357  0.00100771 -0.00000249  0.00000065]
        //return Math.max(0.0, -5.63773557  + M*(0.01784894) + k*(-0.20522357) + M*k*(0.00100771) +  M*M*(-0.00000249) + M*M*k*(0.00000065));
        // V2
        // [ 0.95912032 -0.00169046 -0.0037496   0.00009539  0.00000202  0.0000014 ]
        return Math.max(0.0,  0.95912032  + M*(-0.00169046) + k*(-0.0037496) + M*k*(0.00009539) +  M*M*(0.00000202) + M*M*k*(0.0000014));
    }

    public int getNextNtObjective(int iter, int currNt, int nextNt){
        //M*(lastk^scaling) + MDtime(currNt). Compute and store both predicted and actual
        //changing the objective to being of global time changes this check to be f(kt) + MD(t) < f(k_{t-1}) + 0
        // storing funck-k side as predicted, runtime pred as true.
        double NtTimeGuess = NtTimePredictOneStepGradient(iter, nextNt);

        double prevFk =  kFunc(KList.get(currNt));
        int kGuess = predictK(iter, nextNt); //iter needed for currNt ans one before
        double predFk =  kFunc(kGuess);

        trueObjective.put(currNt, prevFk + dropTimeSoFar);
        predictedObjective.put(nextNt, predFk + NtTimeGuess + dropTimeSoFar);

        fObjective.put(currNt, prevFk - predFk);
        dObjective.put(nextNt, NtTimeGuess);

        // if obj says to keep going OR you're still not feasible OR you are in no opt mode
        if ((prevFk - predFk >= NtTimeGuess) || (!this.feasible) || (!opt)){ //(nextNt <= 1000){ //
            return nextNt;
        }
        return M+1;

    }

    //TODO: check indices here
    public double NtTimePredictOneStepGradient(int iter, int nextNt){
        if (iter == 1){
            double guess = MDDiff + prevMDTime;
            this.predictedTrainTimeList.put(nextNt, guess);
            return guess;
        }
        double ratio = MDDiff/ (NtList.get(iter-1) - NtList.get(iter-2));
        double guess = Math.max(0.0, ratio*(nextNt - NtList.get(iter-1)));
        this.predictedTrainTimeList.put(nextNt, guess + prevMDTime);
        return guess + prevMDTime;
    }

    //predicting K for the "next" iteration and Nt
    public int predictK(int iter, int nextNt){
        if (iter == 1){
            int guess = kDiff + prevK;
            this.kPredList.put(nextNt, guess);
            return guess;
        }
        double ratio = (double) kDiff/ (NtList.get(iter-1) - NtList.get(iter-2));
        int guess = Math.max(0, (int) Math.round(ratio*(nextNt - NtList.get(iter-1))));
        this.kPredList.put(nextNt, guess + prevK);
        return guess + prevK;
    }

    public void updateMDRuntime(int iter, int currNt, double MDtime){
        dropTimeSoFar += MDtime;
        MDruntimes.add(currNt, MDtime);
        trainTimeList.put(currNt, MDtime);

        MDDiff = MDtime - prevMDTime;
        prevMDTime = MDtime;
    }

    public double[] LBRCI(RealMatrix transformedData, int numPairs, double threshold, double constant) {
        int K = transformedData.getColumnDimension();
        //int currNt = NtList.get(iter);

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

        kIndices = Arrays.copyOf(allIndicesN,K);

        transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(Math.sqrt(constant));
        trueDists = this.calcDistances(this.dataMatrix.getSubMatrix(indicesA,allIndicesN), this.dataMatrix.getSubMatrix(indicesB,allIndicesN));
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

    //calls other function with constant set to 1
    public double[] LBRCI(RealMatrix transformedData, int numPairs, double threshold){
       return LBRCI(transformedData, numPairs, threshold, 1.0);
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

    public int getN(){ return N;}

    public int getM(){return M;}

    ///toDO: do somethiing with the first drop information. Maybe just move this to PCAskiing and do this.feasible. Right now this only auto quits with objective function if you didn't improve
    public void setKDiff(int currK){
        kDiff = currK - prevK;
        if (kDiff <= 0){
            this.firstKDrop = false;
        }
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

    public RealMatrix getDataMatrix(){ return dataMatrix; }

    public Map getTrueObjective() { return trueObjective; }

    public Map getPredictedObjective() { return predictedObjective; }

    public Map getfObjectiveCheck() { return fObjective; }

    public Map getdObjectiveCheck() { return dObjective; }

    public abstract void fit(int Nt);

    public abstract RealMatrix transform(int K);

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

    public int[] complexArgSort(Complex[] in, boolean ascending) {
        Integer[] indices = new Integer[in.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (ascending ? 1 : -1) * Double.compare(in[o1].abs(), in[o2].abs());
            }
        });
        return toPrimitive(indices);
    }

    //input must be even length array in [re[0], im[0],...,re[k], im[k]]
    public int[] complexArgSort(double[] in, boolean ascending) {
        Integer[] indices = new Integer[in.length/2];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (ascending ? 1 : -1) * Double.compare(abs(in[2*o1],in[2*o1+1]), abs(in[2*o2],in[2*o2+1]));
            }
        });
        return toPrimitive(indices);
    }

    public double abs(double real, double imaginary) {
        double q;
        if(FastMath.abs(real) < FastMath.abs(imaginary)) {
            if(imaginary == 0.0D) {
                return FastMath.abs(real);
            } else {
                q = real / imaginary;
                return FastMath.abs(imaginary) * FastMath.sqrt(1.0D + q * q);
            }
        } else if(real == 0.0D) {
            return FastMath.abs(imaginary);
        } else {
            q = imaginary / real;
            return FastMath.abs(real) * FastMath.sqrt(1.0D + q * q);
        }
    }

    public int[] argSort(int[] in, boolean ascending) {
        Integer[] indices = new Integer[in.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (ascending ? 1 : -1) * Integer.compare(in[o1], in[o2]);
            }
        });
        return toPrimitive(indices);
    }

    public int[] toPrimitive(Integer[] in) {
        int[] out = new int[in.length];
        for (int i = 0; i < in.length; i++) {
            out[i] = in[i];
        }
        return out;
    }

    public int[] argSort(Double[] in, boolean ascending) {
        Integer[] indices = new Integer[in.length];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (ascending ? 1 : -1) * Double.compare(in[o1], in[o2]);
            }
        });
        return toPrimitive(indices);
    }
}

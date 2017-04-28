package macrobase.analysis.stats.optimizer;


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
    protected double[] MDDiffs;
    protected int prevK;
    protected double prevMDTime;
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

    protected RealMatrix rawDataMatrix;
    protected RealMatrix dataMatrix;

    protected boolean feasible;
    protected boolean firstKDrop;
    protected int lastFeasible;

    protected int Ntdegree;
    protected int kScaling;
    protected PolynomialCurveFitter fitter;
    protected WeightedObservedPoints MDruntimes;

    public SkiingOptimizer(double epsilon) {
        this.numDiffs = 3;
        this.epsilon = epsilon;

        this.NtList = new ArrayList<>();
        this.LBRList = new HashMap<>();
        this.KList = new HashMap<>();
        this.trainTimeList = new HashMap<>();
        this.predictedTrainTimeList = new HashMap<>();
        this.kPredList = new HashMap<>();
        this.kDiffs = new int[this.numDiffs]; //TODO: 3 to change to general param
        this.MDDiffs = new double[this.numDiffs];

        this.prevK = 0;
        this.prevMDTime = 0;
        this.currKCI = new double[]{0, 0, 0};

        this.feasible = false;
        this.firstKDrop = true;
        this.lastFeasible = 0;

        this.kScaling = 2;
        this.Ntdegree = 2;
        this.MDruntimes = new WeightedObservedPoints();
        MDruntimes.add(0, 0);
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

        this.NtInterval = Math.max(10, new Double(this.M*0.1).intValue()); //arbitrary 1%
    }

    public void shuffleData(){
        List<Integer> indicesM = new ArrayList<>();
        int[] indicesN = new int[N];
        for (int i = 0; i < N; i++){
            indicesN[i] = i;
        }
        for (int i = 0; i < M; i++){
            indicesM.add(i);
        }
        Collections.shuffle(indicesM);
        int[] iA = ArrayUtils.toPrimitive(indicesM.toArray(new Integer[M]));

        rawDataMatrix = rawDataMatrix.getSubMatrix(iA, indicesN);
    }

    public void preprocess(){
        Nproc = N;
        dataMatrix = rawDataMatrix;
    }

    public int getNextNtFixedInterval(int iter, int currNt){
        if (iter == 0) {
            return NtInterval;
        }
        return NtInterval + currNt;
    }


    public int getNextNtIncreaseOnly(int iter, int currNt){
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

    public int getNextNtBasicDoubling(int iter, int currNt){
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

    public int getNextNt(int iter, int currNt){
        int nextNt =  getNextNtIncreaseOnly(iter, currNt);
        NtList.add(nextNt);
        return nextNt;
    }

    public int NtTimeGuessFitPoly(int iter, int currNt){
        double[] MDtimeCoeffs = fitter.fit(this.MDruntimes.toList());
        double NtTimeGuess = 0;
        int nextNt = getNextNtIncreaseOnly(iter, currNt); //getNextNtBasicDoubling(iter,currNt,maxNt);

        for (int i = 0; i <= this.Ntdegree; i++){
            NtTimeGuess += MDtimeCoeffs[i]*Math.pow(nextNt, i);
        }
        this.predictedTrainTimeList.put(nextNt, NtTimeGuess);
        return nextNt;
    }

    //TODO: check indices here
    public int NtTimeGuessOneStepGradient(int iter, int currNt){
        int nextNt = getNextNtIncreaseOnly(iter, currNt); //getNextNtBasicDoubling(iter,currNt,maxNt);
        if (iter == 1){
            double guess = MDDiffs[(iter-1) % numDiffs] + prevMDTime;
            this.predictedTrainTimeList.put(nextNt, guess);
            return nextNt;
        }
        double ratio = MDDiffs[(iter-1) % numDiffs]/ (NtList.get(iter-1) - NtList.get(iter-2));
        int guess = (int) Math.round(ratio*(nextNt - NtList.get(iter-1)));
        this.predictedTrainTimeList.put(nextNt, guess + prevMDTime);
        return nextNt;
    }

    //TODO: scale of Nt vs K is off. must normalize
    public int getNextNtObjectiveFunc(int iter, int currNt){
        double prevObjective = Math.pow(KList.get(currNt), kScaling) + trainTimeList.get(currNt);
        double currObjective;
        int nextNt =  NtTimeGuessOneStepGradient(iter, currNt);
        double NtTimeGuess = this.predictedTrainTimeList.get(nextNt);

        int kGuess = predictK(iter, nextNt);
        double kTimeGuess = Math.pow(kGuess,kScaling);

        currObjective = NtTimeGuess*(1./1) + kTimeGuess;

        // giving it a 10% wiggle and first feasible bump
        if ((currObjective <= (1.0)*prevObjective) || (firstKDrop)){ //(nextNt <= 1000){ //
            return nextNt;
        }
        return M+1;
    }

    public int getNextNtPE(int iter, int currNt, boolean attainedLBR){
        int nextNt;
        if (!attainedLBR){
            nextNt = getNextNtIncreaseOnly(iter, currNt);
            NtList.add(nextNt);
            return nextNt;
        }
        nextNt = getNextNtObjectiveFunc(iter, currNt);
        log.debug("NextNt {}", nextNt);
        NtList.add(nextNt);
        return nextNt;
    }

    public int getNextNtFull(int iter, int currNt){
        NtList.add(M);
        return M;
    }



    // this is always called before anything else happens that iter
    public int getNextNtPE(int iter, int currNt){
        // tentative next Nt is this Nt + max{10, 1% data}
        int nextNt = getNextNtFixedInterval(iter, currNt);

        //iter 0 is special because currNt has not been run yet, so no #s exist
        if (iter == 0){
            NtList.add(nextNt);
            return nextNt;
        }

        //for all other iters, MD has been run with currNt
        nextNt = getNextNtObjective(iter, currNt);
        NtList.add(nextNt);
        return nextNt;
    }

    public int getNextNtObjective(int iter, int currNt){
        //lastk^scaling + MDtime(currNt)
        double prevObjective = Math.pow(KList.get(currNt), kScaling) + trainTimeList.get(currNt);
        double currObjective;
        int nextNt =  NtTimePredictOneStepGradient(iter, currNt);
        double NtTimeGuess = this.predictedTrainTimeList.get(nextNt);

        int kGuess = predictK(iter, nextNt);
        double kTimeGuess = Math.pow(kGuess,kScaling);

        currObjective = NtTimeGuess*(1./1) + kTimeGuess;

        // giving it a 10% wiggle and first feasible bump
        if ((currObjective <= (1.0)*prevObjective) || (firstKDrop)){ //(nextNt <= 1000){ //
            return nextNt;
        }
        return M+1;
    }

    //TODO: check indices here
    //Computes and stores the predicted nextNt and time it'll take. Returns nextNt
    public int NtTimePredictOneStepGradient(int iter, int currNt){
        int nextNt = getNextNtFixedInterval(iter, currNt);
        if (iter == 1){
            double guess = MDDiffs[(iter-1) % numDiffs] + prevMDTime;
            this.predictedTrainTimeList.put(nextNt, guess);
            return nextNt;
        }
        double ratio = MDDiffs[(iter-1) % numDiffs]/ (NtList.get(iter-1) - NtList.get(iter-2));
        int guess = (int) Math.round(ratio*(nextNt - NtList.get(iter-1)));
        this.predictedTrainTimeList.put(nextNt, guess + prevMDTime);
        return nextNt;
    }



    //TODO: is the indexed here even right? Also for equivalent basic 1-step
    //predicting K for the "next" iteration and Nt
    public int predictK(int iter, int currNt){
        if (iter == 1){
            int guess = kDiffs[(iter-1) % numDiffs] + prevK;
            this.kPredList.put(currNt, guess);
            return guess;
        }
        double ratio = (double) kDiffs[(iter-1) % numDiffs]/ (NtList.get(iter-1) - NtList.get(iter-2));
        int guess = (int) Math.round(ratio*(currNt - NtList.get(iter-1)));
        this.kPredList.put(currNt, guess + prevK);
        return guess + prevK;
    }

    public void updateMDRuntime(int iter, int currNt, double MDtime){
        MDruntimes.add(currNt, MDtime);
        trainTimeList.put(currNt, MDtime);

        MDDiffs[iter % numDiffs] = MDtime - prevMDTime;
        prevMDTime = MDtime;
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

    ///toDO: do somethiing with the first drop information. Maybe just move this to PCAskiing and do this.feasible. Right now this only auto quits with objective function if you didn't improve
    public void setKDiff(int iter, int currK){
        kDiffs[iter % numDiffs] = currK - prevK;
        if ((currK - prevK) < 0){
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

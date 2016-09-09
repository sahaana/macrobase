package macrobase.analysis.stats.optimizer;


import macrobase.analysis.stats.optimizer.util.PCA;
import macrobase.datamodel.Datum;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.correlation.Covariance;

import java.util.*;

public abstract class SkiingOptimizer {
    protected int N; //original data dimension
    protected int Nproc; //processed data dimension (PAA, Mary, etc)
    protected int M; //original number of training samples

    protected int[] kDiffs;
    protected int prevK;
    protected int numDiffs;
    protected int NtInterval;

    protected List<Integer> NtList;
    protected Map<Integer, Integer> KList;
    protected Map<Integer, double[]> LBRList;
    protected Map<Integer, Double> trainTimeList;

    protected double epsilon;
    protected int s;
    protected int b;

    protected RealMatrix rawDataMatrix;
    protected RealMatrix dataMatrix;

    protected PCA pca;

    public SkiingOptimizer(double epsilon, int b, int s){
        this.numDiffs = 3;
        this.epsilon = epsilon;
        this.s = s;
        this.b = b;

        this.NtList = new ArrayList<>();
        this.LBRList = new HashMap<>();
        this.KList = new HashMap<>();
        this.trainTimeList = new HashMap<>();
        this.kDiffs = new int[this.numDiffs]; //TODO: 3 to change to general param

        this.prevK = 0;
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

        this.NtInterval = Math.max(3, new Double(this.M*0.01).intValue()); //arbitrary 1%
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

    public void preprocess(int reducedDim){
        Nproc = N;
        dataMatrix = rawDataMatrix;
    }

    public int getNextNtFromList(int iter, int currNt, int maxNt){
        //int[] Nts = {11,12,13,14,15,16,17,18,19,20,21,22,23,25,30,35,40,45,50,55,60, 65,70,80,90,100,110,125,150,175,200,300,400,500,600};
        //if (iter == 0){ return 0; }
        //int K =  KList.get(currNt);
        int[] Nts = {10, 20,30,40,50,60,70,80,90,100,110,125,150,175,200};
        if (iter >= Nts.length || NtList.size() >= maxNt) {
            NtList.add(2000000);
            return 2000000;
        }
        NtList.add(Nts[iter]);
        return Nts[iter];
    }

    public int getNextNt(int iter, int currNt, int maxNt){
        //int interval = new Double(M*0.01).intValue(); //arbitrary 1%
        double avgDiff = 0;

        if (iter == 0) {
            NtList.add(NtInterval);
            return NtInterval;
        }

        for (double i: kDiffs){
            avgDiff += i/numDiffs;
        }

        //if things haven't changed much on average, you can stop
        if (avgDiff < .5){
            NtList.add(M+1);
            return M+1;
        }

        //double the interval if it's choking
        if (avgDiff == NtInterval){
            NtInterval = NtInterval*2;
        }

        //halve the interval if it's aight. overshoots tho. lbr-based?
       // if (LBRList.get(currNt) >= )
        if (avgDiff < NtInterval/2){
            NtInterval = new Double(NtInterval/2).intValue();
        }

        NtList.add(NtInterval + currNt);
        return NtInterval+currNt;
    }


    public double[] LBRAttained(int iter, RealMatrix transformedData){
        //if (iter == 0){
        //    return new double[] {0.0, 0.0, 0.0};
        //}
        int currNt = NtList.get(iter);
        int[] allIndices = new int[N]; //bc we compare to raw data matrix
        int K = transformedData.getColumnDimension();
        for (int i = 0; i < N; i++){
            allIndices[i] = i; //TODO: this is stupid
        }
        int[] kIndices = Arrays.copyOf(allIndices,K);

        int num_pairs = (M - currNt)*((M - currNt) - 1)/2;
        int threshL = new Double((epsilon/2)*num_pairs).intValue();
        int threshH = new Double((1 - epsilon/2)*num_pairs).intValue();
        double lower = 0;
        double mean = 0;
        double upper = 0;
        HashMap<List<Integer>, Double> LBRs = new HashMap<>(); //pair -> LBR //change to (pair, LBR)
        Double[][] LBRValCard = new Double[b][2]; //convert to pairs? so list of pairs

        double tLower, tMean, tUpper;
        double lbr;
        double card;
        double currCount;
        int currIter;


        Random rand = new Random();
        int tempA, tempB, tMin, tMax;
        int[] indicesA = new int[b];
        int[] indicesB = new int[b];

        List<Double> tempLBR;
        RealVector transformedDists;
        RealVector trueDists;
        RealVector multinomialVals;

        for (int i = 0; i < s; i++){
            // sample w/out replacement from whole set, b times, so b distinct pairs
            for (int j = 0; j < b; j++){
                tempA = rand.nextInt(this.M - currNt) + currNt;
                tempB = rand.nextInt(this.M - currNt) + currNt;
                tMax = Math.max(tempA, tempB);
                tMin = Math.min(tempA, tempB);
                while (tempA == tempB ||
                        LBRs.containsKey(new ArrayList<>(Arrays.asList(tMin, tMax)))){
                    tempA = rand.nextInt(this.M - currNt) + currNt;
                    tempB = rand.nextInt(this.M - currNt) + currNt;
                    tMax = Math.max(tempA, tempB);
                    tMin = Math.min(tempA, tempB);
                }
                indicesA[j] = tMin;
                indicesB[j] = tMax;
                LBRs.put(new ArrayList<>(Arrays.asList(tMin, tMax)), 0.0);
            }

            // compute lbr over those b distinct pairs.
            // It is super ugly to do this in two separate for loops, but easier to check and stuff
            transformedDists = calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(Math.sqrt(N/Nproc));
            trueDists = calcDistances(rawDataMatrix.getSubMatrix(indicesA,allIndices), rawDataMatrix.getSubMatrix(indicesB,allIndices));
            tempLBR =  calcLBRList(trueDists, transformedDists);
            for (int j = 0; j < b; j++){
                LBRs.put(new ArrayList<>(Arrays.asList(indicesA[j], indicesB[j])), tempLBR.get(j));
            }

            // sample (n choose 2) (or num_pairs) times with replacement from b, use multinomial trick
            multinomialVals = multinomial(num_pairs, b);

            //compute bootstrap Tmean, Tlower, Tupper on b
            tMean = 0;
            for (int j = 0; j < b; j++){
                lbr = LBRs.get(new ArrayList<>(Arrays.asList(indicesA[j], indicesB[j])));
                card  = multinomialVals.getEntry(j);
                tMean += card * lbr;

                LBRValCard[j][0] = lbr;
                LBRValCard[j][1] = card;
            }
            tMean /= num_pairs;

            // sort the lbr-cardinality array by lbr
            Arrays.sort(LBRValCard, new Comparator<Double[]>() {
                @Override
                public int compare(Double[] d1, Double[] d2) {
                    Double k1 = d1[0];
                    Double k2 = d2[0];
                    return k1.compareTo(k2);
                }
            });

            //find %-ile via thresh
            currIter = 0;
            currCount = 0;
            while (currCount < threshL){
                currCount += LBRValCard[currIter++][1];
            }
            tLower = LBRValCard[currIter][0];
            while (currCount < threshH){
                currCount += LBRValCard[currIter++][1];
            }
            tUpper = LBRValCard[currIter][0];

            //update all vals
            mean += tMean/s;
            lower += tLower/s;
            upper += tUpper/s;
        }
        return new double[] {lower, mean,upper};
    }

    public double meanLBR(int iter, RealMatrix transformedData){
        int num_pairs = M;
        int K = transformedData.getColumnDimension();
        int currNt = NtList.get(iter);

        int[] allIndices = new int[this.N];
        int[] indicesA = new int[num_pairs];
        int[] indicesB = new int[num_pairs];
        int[] kIndices;

        Random rand = new Random();

        RealVector transformedDists;
        RealVector trueDists;

        for (int i = 0; i < num_pairs; i++){
            indicesA[i] = rand.nextInt(M - currNt) + currNt;
            indicesB[i] = rand.nextInt(M - currNt) + currNt;
            while(indicesA[i] == indicesB[i]){
                indicesA[i] = rand.nextInt(M - currNt) + currNt;
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

    public void setKDiff(int iter, int currK){
        kDiffs[iter % numDiffs] = Math.abs(currK - prevK);
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

    public Map getLBRList(){ return LBRList; }

    public Map getTrainTimeList(){ return trainTimeList; }

    public Map getKList(){ return KList; }


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

    public  RealVector multinomial(int n, int k){
        RealVector sample = new ArrayRealVector(k);
        RealVector temp;
        Random rand = new Random();
        for (int i = 0; i < n; i++){
            temp = new ArrayRealVector(k);
            temp.setEntry(rand.nextInt(k),1.0);
            sample = sample.add(temp);
        }
        return sample;
    }

}

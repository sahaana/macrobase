package macrobase.analysis.stats.optimizer;


import macrobase.analysis.stats.optimizer.util.PCA;
import macrobase.datamodel.Datum;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.util.*;

public abstract class SkiingOptimizer {
    protected int N; //original data dimension
    protected int Nproc; //processed data dimension (PAA, Mary, etc)
    protected int M; //original number of training samples

    protected List<Integer> NtList;
    protected List<Integer> KList;
    protected List<Double> LBRList;
    protected List<Double> trainTimeList;

    protected double epsilon;
    protected double lbr;

    protected RealMatrix rawDataMatrix;
    protected RealMatrix dataMatrix;

    protected PCA pca;

    public SkiingOptimizer(double epsilon, double lbr){
        this.epsilon = epsilon;
        this.lbr = lbr;

        this.NtList = new ArrayList<>();
        this.LBRList = new ArrayList<>();
        this.KList = new ArrayList<>();
        this.trainTimeList = new ArrayList<>();
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

    public void train(int Nt){
        RealMatrix trainMatrix = dataMatrix.getSubMatrix(0, Nt-1, 0, Nproc-1);
        pca = new PCA(trainMatrix);
    }

    public RealMatrix transform(int K){
        return pca.transform(dataMatrix, K);
    }

    public int getNextNt(int iter, int maxNt){
        //int[] Nts = {11,12,13,14,15,16,17,18,19,20,21,22,23,25,30,35,40,45,50,55,60, 65,70,80,90,100,110,125,150,175,200,300,400,500,600};
        int K =  KList.get(KList.size()-1);
        int[] Nts = {21,22,23,25,30,35,40,45,50,55,60, 65,70,80,90,100,110,125,150,175,200,300,400,500,600};
        if (iter >= Nts.length || NtList.size() >= maxNt) {
            NtList.add(2000000);
            return 2000000;
        }
        NtList.add(Math.max(K+1,Nts[iter]));
        return Math.max(K+1,Nts[iter]);
    }


    public double[] blbLBRAttained(int iter, double epsilon, RealMatrix transformedData, int b, int s){
        if (iter == 0){
            return new double[] {0.0, 0.0, 0.0};
        }
        int currNt = this.getNtList(iter);
        int[] allIndices = new int[this.N];
        int K = transformedData.getColumnDimension();
        for (int i = 0; i < this.N; i++){
            allIndices[i] = i; //TODO: this is stupid
        }
        int[] kIndices = Arrays.copyOf(allIndices,K);

        int num_pairs = (this.M - currNt)*((this.M - currNt) - 1)/2;
        int threshL = new Double((epsilon/2)*num_pairs).intValue();
        int threshH = new Double((1 - epsilon/2)*num_pairs).intValue();
        double lower = 0;
        double mean = 0;
        double upper = 0;
        HashMap<List<Integer>, Double> LBRs = new HashMap<>(); //pair -> distance
        Double[][] LBRValCard = new Double[b][2];

        double tLower, tMean, tUpper;
        double lbr;
        double card;
        double currCount;
        int currIter;


        Random rand = new Random();
        int[] indicesA = new int[b];
        int[] indicesB = new int[b];

        List<Double> tempLBR;
        RealVector transformedDists;
        RealVector trueDists;
        RealVector multinomialVals;

        for (int i = 0; i < s; i++){
            // sample w/out replacement from whole set, b times, so b distinct pairs
            for (int j = 0; j < b; j++){
                indicesA[j] = rand.nextInt(this.M - currNt) + currNt;
                indicesB[j] = rand.nextInt(this.M - currNt) + currNt;
                while (indicesA[j] == indicesB[j] ||
                        LBRs.containsKey(new ArrayList<>(Arrays.asList(indicesA[j], indicesB[j]))) ||
                        LBRs.containsKey(new ArrayList<>(Arrays.asList(indicesB[j], indicesA[j])))){
                    indicesA[j] = rand.nextInt(this.M - currNt) + currNt;
                    indicesB[j] = rand.nextInt(this.M - currNt) + currNt;
                }
                LBRs.put(new ArrayList<>(Arrays.asList(indicesA[j], indicesB[j])), 0.0);
            }
            // compute lbr over those b distinct pairs.
            // It is super ugly to do this in two separate for loops, but easier to check and stuff
            transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(Math.sqrt(this.N/this.Nproc));
            trueDists = this.calcDistances(this.rawDataMatrix.getSubMatrix(indicesA,allIndices), this.rawDataMatrix.getSubMatrix(indicesB,allIndices));
            tempLBR =  this.LBRList(trueDists, transformedDists);
            for (int j = 0; j < b; j++){
                LBRs.put(new ArrayList<>(Arrays.asList(indicesA[j], indicesB[j])), tempLBR.get(j));
            }

            // sample (n choose 2) (or num_pairs) times with replacement from b, use multinomial trick
            multinomialVals = this.multinomial(num_pairs, b);

            //compute bootstrap Tmean, Tlower, Tupper on b
            tMean = 0;
            tLower = 0;
            tUpper = 0;
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

            mean += tMean/s;
            lower += tLower/s;
            upper += tUpper/s;

        }
        //return mean;
        return new double[] {lower, mean,upper};
    }











}

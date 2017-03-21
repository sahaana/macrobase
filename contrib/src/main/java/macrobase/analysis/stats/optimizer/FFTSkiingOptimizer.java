package macrobase.analysis.stats.optimizer;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.transform.DftNormalization;
import org.apache.commons.math3.transform.FastFourierTransformer;
import org.apache.commons.math3.transform.TransformType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FFTSkiingOptimizer extends SkiingOptimizer {
    private static final Logger log = LoggerFactory.getLogger(FFTSkiingOptimizer.class);
    protected Map<Integer, Integer> KItersList;

    protected FastFourierTransformer transformer;

    protected RealMatrix paddedInput;
    protected Complex[][] transformedData;
    protected int nextPowTwo;

    public FFTSkiingOptimizer(double epsilon, int b, int s) {
        super(epsilon, b, s);
        this.KItersList = new HashMap<>();
        transformer = new FastFourierTransformer(DftNormalization.STANDARD);
    }

    @Override
    public void fit(int Nt) {
        nextPowTwo = Math.max(2, 2 * Integer.highestOneBit(this.N - 1));

        transformedData = new Complex[this.M][nextPowTwo];//Array2DRowRealMatrix(this.M, nextPowTwo);
        paddedInput = new Array2DRowRealMatrix(this.M, nextPowTwo);
        paddedInput.setSubMatrix(this.dataMatrix.getData(), 0, 0);

        for (int i = 0; i < this.M; i++) {
            transformedData[i] = transformer.transform(paddedInput.getRow(i), TransformType.FORWARD);
        }

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

    public void test() {
        int i = 891;
        int j = 1879;
        //int use = this.nextPowTwo;///16;
        for (int use = nextPowTwo; use >0 ; use-=10) {
            RealVector pointAOrig = this.dataMatrix.getRowVector(i);
            RealVector pointBOrig = this.dataMatrix.getRowVector(j);
            Double distOrig = pointAOrig.getDistance(pointBOrig);

            RealVector pointA = new ArrayRealVector();//use*2);
            RealVector pointB = new ArrayRealVector();//use*2);
            for (int k = 0; k < use; k++) {
                pointA = pointA.append(this.transformedData[i][k].getReal());
                pointA = pointA.append(this.transformedData[i][k].getImaginary());
                pointB = pointB.append(this.transformedData[j][k].getReal());
                pointB = pointB.append(this.transformedData[j][k].getImaginary());
            }

            Double distNew = pointA.getDistance(pointB);
            Double distOther = getDist(Arrays.copyOf(transformedData[i], use), Arrays.copyOf(transformedData[j], use));
            double constant = (Math.sqrt(this.N) / Math.sqrt(this.N * this.N));//*Math.sqrt(use/this.nextPowTwo) ;
            //constant *= Math.sqrt(this.nextPowTwo)/Math.sqrt(use);//2*this.nextPowTwo/pointA.getDimension());
            log.debug("orig {} new {} ratio {}", distOrig, distNew, (constant * distNew) / distOrig);

            RealMatrix trialA = new Array2DRowRealMatrix(1,this.N);
            RealMatrix trialB = new Array2DRowRealMatrix(1, this.N);
            trialA.setRowVector(0,pointAOrig);
            trialB.setRowVector(0,pointBOrig);

            RealMatrix trialAA = this.transform(use*2).getRowMatrix(i);
            RealMatrix trialBB = this.transform(use*2).getRowMatrix(j);
            //trialAA.setRowVector(0,pointA);
            //trialBB.setRowVector(0,pointB);

            distNew = calcDistances(trialAA, trialBB).getEntry(0);
            distOrig = calcDistances(trialA, trialB).getEntry(0);
            log.debug("orig {} now {} w/ ratio {}", distOrig, distNew, constant*distNew/distOrig);

        }
    }

    public void test(int i,int j, int[] kIndices, RealMatrix td) {
        //int use = this.nextPowTwo;///16;
        for (int use = nextPowTwo; use >1022; use-=10) {
            RealVector pointAOrig = this.dataMatrix.getRowVector(i);
            RealVector pointBOrig = this.dataMatrix.getRowVector(j);
            Double distOrig = pointAOrig.getDistance(pointBOrig);

            RealVector pointA = new ArrayRealVector();//use*2);
            RealVector pointB = new ArrayRealVector();//use*2);
            for (int k = 0; k < use; k++) {
                pointA = pointA.append(this.transformedData[i][k].getReal());
                pointA = pointA.append(this.transformedData[i][k].getImaginary());
                pointB = pointB.append(this.transformedData[j][k].getReal());
                pointB = pointB.append(this.transformedData[j][k].getImaginary());
            }

            double distNew = pointA.getDistance(pointB);
            double distOther = getDist(Arrays.copyOf(transformedData[i], use), Arrays.copyOf(transformedData[j], use));
            double constant = (Math.sqrt(this.N) / Math.sqrt(this.N * this.N));//*Math.sqrt(use/this.nextPowTwo) ;
            //constant *= Math.sqrt(this.nextPowTwo)/Math.sqrt(use);//2*this.nextPowTwo/pointA.getDimension());
            log.debug("orig {} new {} ratio {}", distOrig, distNew, (constant * distNew) / distOrig);

            RealMatrix trialA = new Array2DRowRealMatrix(1,this.N);
            RealMatrix trialB = new Array2DRowRealMatrix(1, this.N);
            trialA.setRowVector(0,pointAOrig);
            trialB.setRowVector(0,pointBOrig);

            RealMatrix trialAA = this.transform(use*2).getRowMatrix(i);
            RealMatrix trialBB = this.transform(use*2).getRowMatrix(j);
            //trialAA.setRowVector(0,pointA);
            //trialBB.setRowVector(0,pointB);

            distNew = calcDistances(trialAA, trialBB).getEntry(0);
            distOrig = calcDistances(trialA, trialB).getEntry(0);
            distOther = calcDistances(td.getSubMatrix(new int[] {i},kIndices), td.getSubMatrix(new int[] {j}, kIndices)).getEntry(0);
            for (int p = 0; p < 2048; p++){
                if (trialAA.getEntry(0,p) != td.getSubMatrix(new int[] {i},kIndices).getEntry(0,p)){
                    System.out.println(td.getSubMatrix(new int[] {i},kIndices).getEntry(0,p));
                    System.out.println(trialAA.getEntry(0,p));
                }
                if (trialBB.getEntry(0,p) != td.getSubMatrix(new int[] {j},kIndices).getEntry(0,p)){
                    System.out.println(td.getSubMatrix(new int[] {j},kIndices).getEntry(0,p));
                    System.out.println(trialBB.getEntry(0,p));
                }
            }

            log.debug("orig {} now {} other {}", distOrig, constant*distNew, constant*distOther);

        }
    }

    public double getDist(Complex[] c1, Complex[] c2){
        double dist = 0;//new Complex(0);
        for (int i = 0; i < c1.length; i++) {
            dist += Math.pow(c1[i].subtract(c2[i]).abs(),2);//.getReal();//.sqrt().getReal();
        }
        return Math.sqrt(dist);
    }


    @Override
    //K must be even.
    public RealMatrix transform(int K) {
        assert (K % 2 == 0);

        RealMatrix output = new Array2DRowRealMatrix(this.M, K);
        int[] sortedIndices;
        int[] freqCounts = new int[this.nextPowTwo];
        int[] topFreqs;

        RealVector tempOut;
        Complex[] curr;

        //Computing the top K/2 frequencies for each data point, then putting that in a count array to see which are most frequent of those
        for (int i = 0; i < this.M; i++) {
            sortedIndices = Arrays.copyOfRange(complexArgSort(transformedData[i], false), 0, K / 2);
            for (int j : sortedIndices) {
                freqCounts[j] += 1;
            }
        }
        topFreqs = Arrays.copyOfRange(argSort(freqCounts, false), 0, K / 2);

        /*topFreqs = new int[K/2];
        for (int i = 0; i < K/2 ; i++){
            topFreqs[i] = i;
        }*/

        for (int i = 0; i < this.M; i++) {
            curr = transformedData[i];
            tempOut = new ArrayRealVector();
            for (int f : topFreqs) {
                tempOut = tempOut.append(curr[f].getReal());
                tempOut = tempOut.append(curr[f].getImaginary());
            }
            output.setRowVector(i, tempOut);
        }
        return output;
    }

    public RealMatrix getKBin(int iter, double targetLBR) {
        //confidence interval based method for getting K
        double LBR;
        RealMatrix currTransform;
        double[] CI;

        int iters = 0;
        int low = 0;
        int high = this.nextPowTwo; //TODO: how high should this go?
        if (this.feasible) high = this.lastFeasible;
        int mid = (low + high) / 2;

        this.Nproc = this.N*this.N; //TODO: what should this be?
        while (low < high) {
            currTransform = this.transform(2*mid);
            LBR = evalK(targetLBR, currTransform);//this.LBRCI(currTransform, numPairs, thresh)[0];
            CI = this.LBRCI(currTransform, M, 1.96);
            log.debug("With K {}, LBR {} {} {}", mid*2, CI[0], CI[1],CI[2]);
            if (targetLBR < LBR) {
                currTransform = this.transform(2*(mid - 1));
                LBR = evalK(targetLBR, currTransform);//this.LBRCI(currTransform, numPairs, thresh)[0];
                CI = this.LBRCI(currTransform, M, 1.96);
                log.debug("With K {}, LBR {} {} {}", 2*mid, CI[0], CI[1],CI[2]);
                if (targetLBR > LBR) {
                    this.feasible = true;
                    this.lastFeasible = 2*mid;
                    KItersList.put(this.NtList.get(iter), iters);
                    return this.transform(2*mid);
                }
                high = mid - 1;
            } else if (targetLBR > LBR) {
                low = mid + 1;
            } else {
                high = mid;
            }
            iters += 1;
            mid = (low + high) / 2;
        }
        this.feasible = true;
        this.lastFeasible = 2*mid;
        KItersList.put(this.M, iters);
        currTransform = this.transform(2 * mid);
        CI = this.LBRCI(currTransform, M, 1.96);
        log.debug("With K {}, LBR {} {} {}", 2 * mid, CI[0], CI[1], CI[2]);
        return currTransform;
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        //confidence interval based method for getting K
        double LBR;
        RealMatrix currTransform;
        double[] CI;

        this.Nproc = this.N*this.N;

        for (int i = 2; i <= this.nextPowTwo*2; i+=4){
            currTransform = this.transform(i);
            LBR  = evalK(targetLBR, currTransform);
            CI = this.LBRCI(currTransform, M, 1.96);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1],CI[2]);
            if (targetLBR <= LBR) {
                this.feasible = true;
                this.lastFeasible = i;
                return currTransform;
            }
        }
        return this.transform(this.nextPowTwo*2);
    }

    private double evalK(double LBRThresh, RealMatrix currTransform){
        double[] CI;
        double q = 1.96;
        double prevMean = 0;
        int numPairs = (this.M)*((this.M) - 1)/2;
        int currPairs = 100;//Math.max(5, this.M);//new Double(0.005*numPairs).intValue());
        while (currPairs < numPairs){
            CI = this.LBRCI(currTransform,currPairs, q);
            if (CI[0] > LBRThresh){
                return LBRThresh;
            }
            else if (CI[2] < LBRThresh){
                return 0.0;
            }
            else if (Math.abs(CI[1]-prevMean) < .02){
                return 0.0;
            }
            else {
                currPairs *= 2;
                prevMean = CI[1];
            }
        }
        return 0.0;
    }



    public Map<Integer, Double> computeLBRs(){
        //confidence interval based method for getting K
        Map<Integer, Double> LBRs = new HashMap<>();
        double[] CI = {0,0,0};
        int interval = Math.max(2,this.N/32 + ((this.N/32) % 2)); //ensure even k always
        RealMatrix currTransform;
        this.Nproc = this.N*this.N;
        for (int i = 2;((i <= this.N) && (CI[1] <= .99)); i+= interval){
            currTransform = this.transform(i);
            CI = this.LBRCI(currTransform, M, 1.96);
            log.debug("With K {}, LBR {} {} {}", i, CI[0], CI[1],CI[2]);
            LBRs.put(i, CI[1]);
        }
        return LBRs;
    }

    @Override
    public double[] LBRCI(RealMatrix transformedData, int numPairs, double threshold){
        //int numPairs = M;
        int K = transformedData.getColumnDimension();
        //int currNt = NtList.get(iter);

        int[] indices = new int[this.nextPowTwo * 2];
        int[] allIndices;
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

        for (int i = 0; i < this.nextPowTwo*2; i++){
            indices[i] = i; //TODO: FIXME: 9/2/16
        }
        kIndices = Arrays.copyOf(indices,K);
        allIndices = Arrays.copyOf(indices,this.nextPowTwo);

        // RealMatrix pooper = transformedData.getSubMatrix(indicesA,kIndices);
        transformedDists = this.calcDistances(transformedData.getSubMatrix(indicesA,kIndices), transformedData.getSubMatrix(indicesB, kIndices)).mapMultiply(1/Math.sqrt(this.nextPowTwo));//.mapMultiply(Math.sqrt(this.N)/Math.sqrt(this.N*this.N));
        trueDists = this.calcDistances(this.paddedInput.getSubMatrix(indicesA,allIndices), this.paddedInput.getSubMatrix(indicesB,allIndices));

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

    @Override
    public int getNextNt(int iter, int currNt, int maxNt) {
        return this.M;
    }

    public Map getKItersList(){ return KItersList; }

}

package macrobase.analysis.stats.optimizer;

import macrobase.analysis.stats.optimizer.util.PCA;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.*;

import java.util.HashMap;
import java.util.Map;

public class PCASkiingOptimizer extends SkiingOptimizer {

    protected Map<Integer, Integer> KItersList;

    public PCASkiingOptimizer(double epsilon, int b, int s){
        super(epsilon, b, s);
        this.KItersList = new HashMap<>();
    }

    @Override
    public void fit(int Nt) {
        RealMatrix trainMatrix = dataMatrix.getSubMatrix(0, Nt-1, 0, Nproc-1);
        this.pca = new PCA(trainMatrix);
    }

    public void maryFit(int Nt){
        RealVector mean = new ArrayRealVector(Nproc);
        RealVector covV = new ArrayRealVector(Nproc, 1d/Nproc);
        RealMatrix covM = new DiagonalMatrix(covV.toArray());
        MultivariateNormalDistribution  mnd = new MultivariateNormalDistribution(mean.toArray(), covM.getData());
        //mnd.reseedRandomGenerator(randomSeed);
        RealMatrix randomProjectionMatrix = new Array2DRowRealMatrix(mnd.sample(Nt));
        RealMatrix trainMatrix = dataMatrix.multiply(randomProjectionMatrix);
        this.pca = new PCA(trainMatrix);
    }

    @Override
    public RealMatrix transform(int K) {
        return this.pca.transform(dataMatrix, K);
    }


    public RealMatrix getKFull(double targetLBR){
        // computes the best K for a complete transformation using all the data
        double LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        int iters = 0;
        int iter = 8008;
        int low = 0;
        int high = this.Nproc - 1;
        int mid = (low + high) / 2;

        //System.out.println(this.M);
        currTransform = this.transform(high);
        LBR = this.meanLBR(iter, currTransform);
        if (targetLBR > LBR){
            return currTransform;
        }

        while (low < high) {
            currTransform = this.transform(mid);
            LBR = this.meanLBR(iter, currTransform);
            if (targetLBR < LBR) {
                currTransform = this.transform(mid - 1);
                LBR = this.meanLBR(iter, currTransform);
                if (targetLBR > LBR) {
                    return currTransform;
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
        return this.transform(mid);
    }

    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        //simply uses the mean LBR to do K computations with
        double LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        int iters = 0;
        int low = 0;
        int high = Math.min(this.Nproc, this.NtList.get(iter)) - 1;
        int mid = (low + high) / 2;

        //System.out.println(this.M);
        currTransform = this.transform(high);
        LBR = this.meanLBR(iter, currTransform);
        if (targetLBR > LBR){
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
                    KItersList.put(this.NtList.get(iter), iters);
                    return currTransform;
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
        KItersList.put(this.NtList.get(iter), iters);
        return this.transform(mid);
    }

    public RealMatrix getKCI(int iter, double targetLBR) {
        //confidence interval based method for getting K
        double LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        double thresh = 1.96;
        int numPairs = new Double(0.01*(this.M)*((this.M) - 1)/2).intValue();

        int iters = 0;
        int low = 0;
        int high = Math.min(this.Nproc, this.NtList.get(iter)) - 1;
        int mid = (low + high) / 2;

        //System.out.println(this.M);
        currTransform = this.transform(high);
        LBR = evalK(thresh, currTransform);//this.LBRCI(currTransform,numPairs, thresh)[0];
        if (targetLBR > LBR){
            KItersList.put(this.NtList.get(iter), ++iters);
            return currTransform;
        }

        while (low < high) {
            currTransform = this.transform(mid);
            LBR = evalK(thresh, currTransform);//this.LBRCI(currTransform, numPairs, thresh)[0];
            if (targetLBR < LBR) {
                currTransform = this.transform(mid - 1);
                LBR = evalK(thresh, currTransform);//this.LBRCI(currTransform, numPairs, thresh)[0];
                if (targetLBR > LBR) {
                    KItersList.put(this.NtList.get(iter), iters);
                    return currTransform;
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
        KItersList.put(this.NtList.get(iter), iters);
        return this.transform(mid);
    }

    private double evalK(double thresh, RealMatrix currTransform){
        double[] CI;
        double prevMean = 0;
        int numPairs = (this.M)*((this.M) - 1)/2;
        int currPairs = new Double(0.1*numPairs).intValue();
        while (currPairs < numPairs){
            CI = this.LBRCI(currTransform,currPairs, thresh);
            if (CI[0] > thresh){
                return thresh;
            }
            else if (CI[2] < thresh){
                return 0.0;
            }
            else if (CI[1]-prevMean < .02){
                return 0.0;
            }
            else {
                currPairs *= 2;
                prevMean = CI[1];
            }
        }
        return 0.0;
    }


/*    @Override
    public RealMatrix getK(int iter, double targetLBR) {
        int mid;
        double LBR;
        RealMatrix currTransform; //= new Array2DRowRealMatrix();

        int iters = 0;
        int low = 0;
        int high = Math.min(this.Nproc, this.NtList.get(iter)) - 1;
        //System.out.println(this.M);

        while (low <= high) {
            mid = (low + high) / 2;
            currTransform = this.transform(mid);
            LBR = this.meanLBR(iter, currTransform);
            if (targetLBR < LBR) {
                currTransform = this.transform(mid - 1);
                LBR = this.meanLBR(iter, currTransform);
                if (targetLBR > LBR) {
                    KItersList.put(this.NtList.get(iter), iters);
                    return currTransform;
                }
                high = mid - 1;
            } else if (targetLBR > LBR) {
                low = mid + 1;
            } else {
                KItersList.put(this.NtList.get(iter), iters);
                return currTransform;
            }
            iters += 1;
        }
        KItersList.put(this.NtList.get(iter), iters);
        return this.transform(Math.min(this.Nproc, this.NtList.get(iter)) - 1);
    } */

    public Map getKItersList(){ return KItersList; }
        /*
        RealMatrix currTransform = new Array2DRowRealMatrix();
        double LBR;
        for (int i = 5; i < Math.min(this.Nproc, this.NtList.get(iter)); i++){
            currTransform = this.transform(i);
            LBR = this.meanLBR(iter, currTransform);
            if (LBR >= targetLBR){
                System.out.println(LBR);
                break;
            }
        }
        return currTransform;
    } */
}

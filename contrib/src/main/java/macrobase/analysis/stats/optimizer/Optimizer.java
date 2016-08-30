package macrobase.analysis.stats.optimizer;

import Jama.Matrix;
import macrobase.datamodel.Datum;

import java.util.*;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public abstract class Optimizer {
    protected  int N; //original data dimension
    protected int Nproc; //preprocessed dimension
    protected int M; //orig number of training samples
    protected ArrayList<Integer> NtList;
    protected Map<Integer, Double> LBRList;
    protected Map<Integer, Double> trainTimeList;
    protected double epsilon;
    protected double lbr;
    protected RealMatrix rawDataMatrix;
    protected RealMatrix dataMatrix;

    public Optimizer(double epsilon, double lbr){
        this.epsilon = epsilon;
        this.lbr = lbr;
        this.NtList = new ArrayList<>();
        this.LBRList = new HashMap<>();
        this.trainTimeList = new HashMap<>();
    }

    public int getNproc(){return Nproc;}

    public int getN(){ return N;}

    public int getM(){return M;}

    public void setLBRList(int k, double v){
        LBRList.put(k, v);
    }

    public void setTrainTimeList(int k, double v){
        trainTimeList.put(k, v);
    }

    public Map getLBRList(){ return LBRList; }

    public Map getTrainTimeList(){ return trainTimeList; }

    public void printData(int i0, int i1, int j0, int j1){
        (new Matrix(dataMatrix.getSubMatrix(i0,i1,j0,j1).getData())).print(5,8);
    }

    public int getNtList(int i){return NtList.get(i);}

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

    //TODO: this should really just call LBRList
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

    public List<Double> LBRList(RealVector trueDists, RealVector transformedDists){
        int num_entries = trueDists.getDimension();
        List<Double> lbr = new ArrayList<>();
        for (int i = 0; i < num_entries; i++) {
            if (transformedDists.getEntry(i) == 0){
                if (trueDists.getEntry(i) == 0) lbr.add(1.0); //they were same to begin w/, so max of 1
                else lbr.add(0.0); //can never be negative, so lowest
            }
            lbr.add(transformedDists.getEntry(i)/trueDists.getEntry(i));
        }
        return lbr;
    }

    //TODO: Move this to a util class or something. Silly to have this here
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

    //arguments for both, why min and why max
    public void preprocess(int minReducedDim){
        Nproc = N;
        dataMatrix = rawDataMatrix;
    }

    public abstract RealMatrix transform(int K, int Nt);

    public abstract double LBRAttained(int iter, double epsilon, RealMatrix transformedMatrix);

    public abstract int getNextNt(int iter, int K, int num_Nt);
}

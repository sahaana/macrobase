package macrobase.analysis.stats.optimizer.util;

import org.apache.commons.math3.linear.RealMatrix;

public interface PCA {

    int getN();

    int getM();

    RealMatrix getTransformationMatrix();

    RealMatrix transform(RealMatrix inputData, int K);

}

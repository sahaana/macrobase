#!/usr/bin/env python
import subprocess

datasets = ["50words", "Adiac", "ArrowHead", "Beef", "BeetleFly", "BirdChicken", "Car", "CBF", "ChlorineConcentration", "CinC", "Coffee", "Computers", "Cricket", "Cricket", "Cricket", "DiatomSizeReduction", "DistalPhalanxOutlineAgeGroup", "DistalPhalanxOutlineCorrect", "DistalPhalanxTW", "Earthquakes", "ECG200", "ECG5000", "ECGFiveDays", "ElectricDevices", "FaceAll", "FaceFour", "FacesUCR", "FISH", "FordA", "FordB", "Gun", "Ham", "HandOutlines", "Haptics", "Herring", "InlineSkate", "InsectWingbeatSound", "ItalyPowerDemand", "LargeKitchenAppliances", "Lighting2", "Lighting7", "MALLAT", "Meat", "MedicalImages", "MiddlePhalanxOutlineAgeGroup", "MiddlePhalanxOutlineCorrect", "MiddlePhalanxTW", "MoteStrain", "NonInvasiveFatalECG", "NonInvasiveFatalECG", "OliveOil", "OSULeaf", "PhalangesOutlinesCorrect", "Phoneme", "Plane", "ProximalPhalanxOutlineAgeGroup", "ProximalPhalanxOutlineCorrect", "ProximalPhalanxTW", "RefrigerationDevices", "ScreenType", "ShapeletSim", "ShapesAll", "SmallKitchenAppliances", "SonyAIBORobotSurface", "SonyAIBORobotSurfaceII", "StarLightCurves", "Strawberry", "SwedishLeaf", "Symbols", "synthetic", "ToeSegmentation1", "ToeSegmentation2", "Trace", "TwoLeadECG", "Two", "UWaveGestureLibraryAll", "uWaveGestureLibrary", "uWaveGestureLibrary", "uWaveGestureLibrary", "wafer", "Wine", "WordsSynonyms", "Worms", "WormsTwoClass", "yoga"]

q = 1.96
kExp = 3

proc = "java -xms6g ${JAVA_OPTS} -cp 'assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes' macrobase.analysis.stats.optimizer.experiments.LBRvRuntimeExperiments %s %f %d"
comp = "mvn compile -Dmaven.test.skip=true"
package = "mvn package -DskipTests"
subprocess.call(comp, shell=True)
subprocess.call(package, shell=True)

for dataset in datasets:
    subprocess.call(proc % (dataset, q, kExp), shell=True)

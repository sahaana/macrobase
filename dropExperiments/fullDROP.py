#!/usr/bin/env python
import subprocess

datasets = ['50words', 'Adiac', 'CBF', 'ChlorineConcentration', 'CinC',
       'Cricket', 'DistalPhalanxOutlineAgeGroup',
       'DistalPhalanxOutlineCorrect', 'DistalPhalanxTW', 'ECG5000',
       'ECGFiveDays', 'ElectricDevices', 'FaceAll', 'FacesUCR', 'FordA',
       'FordB', 'HandOutlines', 'InlineSkate', 'InsectWingbeatSound',
       'ItalyPowerDemand', 'LargeKitchenAppliances', 'MALLAT',
       'MedicalImages', 'MiddlePhalanxOutlineAgeGroup',
       'MiddlePhalanxOutlineCorrect', 'MiddlePhalanxTW', 'MoteStrain',
       'NonInvasiveFatalECG', 'PhalangesOutlinesCorrect', 'Phoneme',
       'ProximalPhalanxOutlineAgeGroup', 'ProximalPhalanxOutlineCorrect',
       'ProximalPhalanxTW', 'RefrigerationDevices', 'ScreenType',
       'ShapesAll', 'SmallKitchenAppliances', 'SonyAIBORobotSurface',
       'SonyAIBORobotSurfaceII', 'StarLightCurves', 'Strawberry',
       'SwedishLeaf', 'Symbols', 'Two', 'TwoLeadECG',
       'UWaveGestureLibraryAll', 'WordsSynonyms', 'synthetic',
       'uWaveGestureLibrary', 'wafer', 'yoga','MNIST_all']

lbr = 0.98
q = 1.96
work = "REUSE"
opt = "OPTIMIZE"
numTrials = 200


proc = "java -Xms6g ${JAVA_OPTS} -cp 'assembly/target/*:core/target/classes:frontend/target/classes:contrib/target/classes' macrobase.analysis.stats.optimizer.experiments.FullDROPExperiments %s %f %f %s %s %d"
comp = "mvn compile -Dmaven.test.skip=true"
package = "mvn package -DskipTests"
subprocess.call(comp, shell=True)
subprocess.call(package, shell=True)

for dataset in datasets:
    subprocess.call(proc % (dataset, lbr, q, work, opt, numTrials), shell=True)


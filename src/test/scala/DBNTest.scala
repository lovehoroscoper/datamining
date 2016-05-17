import java.util.Collections

//import org.deeplearning4j.datasets.iterator.impl.MnistDataSetIterator
//import org.deeplearning4j.eval.Evaluation
//import org.deeplearning4j.nn.api.OptimizationAlgorithm
//import org.deeplearning4j.nn.conf.layers.{OutputLayer, RBM}
//import org.deeplearning4j.nn.conf.{GradientNormalization, NeuralNetConfiguration}
//import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
//import org.deeplearning4j.nn.weights.WeightInit
//import org.deeplearning4j.optimize.api.IterationListener
//import org.deeplearning4j.optimize.listeners.ScoreIterationListener
//import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction
//import org.slf4j.LoggerFactory

/**
  * Created by xiaonuo on 5/16/16.
  */
class DBNTest {

//  lazy val logger = LoggerFactory.getLogger(classOf[DBNTest])

  def main(args: Array[String]) = {
//    val numRows = 28
//    val numColumns = 28
//    val outputNum = 10
//    val numSamples = 60000
//    val batchSize = 100
//    val iterations = 10
//    val seed = 123
//    val listenerFreq = batchSize / 5
//
//    logger.info("Load data....")
//    val iter = new MnistDataSetIterator(batchSize, numSamples, true)
//
//    logger.info("Build model....")
//    val conf = new NeuralNetConfiguration.Builder()
//      .seed(seed)
//      .gradientNormalization(GradientNormalization.ClipElementWiseAbsoluteValue)
//      .gradientNormalizationThreshold(1.0)
//      .iterations(iterations)
//      .momentum(0.5)
//      .momentumAfter(Collections.singletonMap(3, 0.9))
//      .optimizationAlgo(OptimizationAlgorithm.CONJUGATE_GRADIENT)
//      .list(4)
//      .layer(0, new RBM.Builder().nIn(numRows * numColumns).nOut(500)
//        .weightInit(WeightInit.XAVIER).lossFunction(LossFunction.RMSE_XENT)
//        .visibleUnit(RBM.VisibleUnit.BINARY)
//        .hiddenUnit(RBM.HiddenUnit.BINARY)
//        .build())
//      .layer(1, new RBM.Builder().nIn(500).nOut(250)
//        .weightInit(WeightInit.XAVIER).lossFunction(LossFunction.RMSE_XENT)
//        .visibleUnit(RBM.VisibleUnit.BINARY)
//        .hiddenUnit(RBM.HiddenUnit.BINARY)
//        .build())
//      .layer(2, new RBM.Builder().nIn(250).nOut(200)
//        .weightInit(WeightInit.XAVIER).lossFunction(LossFunction.RMSE_XENT)
//        .visibleUnit(RBM.VisibleUnit.BINARY)
//        .hiddenUnit(RBM.HiddenUnit.BINARY)
//        .build())
//      .layer(3, new OutputLayer.Builder(LossFunction.NEGATIVELOGLIKELIHOOD).activation("softmax")
//        .nIn(200).nOut(outputNum).build())
//      .pretrain(true).backprop(false)
//      .build()
//
//    val model = new MultiLayerNetwork(conf)
//    model.init()
//    model.setListeners(Collections.singletonList(new ScoreIterationListener(listenerFreq).asInstanceOf[IterationListener]))
//
//    logger.info("Train model....")
//    model.fit(iter); // achieves end to end pre-training
//
//    logger.info("Evaluate model....")
//    val eval = new Evaluation(outputNum)
//
//    val testIter = new MnistDataSetIterator(100, 10000)
//    while (testIter.hasNext) {
//      val testMnist = testIter.next()
//      val predict2 = model.output(testMnist.getFeatureMatrix)
//      eval.eval(testMnist.getLabels, predict2)
//    }
//
//    logger.info(eval.stats())
//    logger.info("****************Example finished********************")
  }
}

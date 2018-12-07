ClassificationModel

* transform(dataset): Dataframe

  Transforms calls all the following methods (depending on rawPredictionCol and predictionCol being defined)

  * predictRawpredictRaw(features: FeaturesType): Vector

  * ```scala
    Raw prediction for each possible label.
    
    ONLY REQUIRED METHOD TO IMPLEMENT
    
    In our case (implementing binary multilabel) we will have to take only the prediction for label '1' of each model, ignoring the one for label '0'
    ```

  * raw2prediction(rawPrediction: Vector): Double

    ```scala
    Given a vector of raw predictions, select the predicted label.
    
    We a problem here, in our case this method has to return an array of labels, we need to override this.
    ```

  * predict(features: FeaturesType): Double 

    This method is implemented using predictRaw and raw2prediction

    ```scala
    Predict label for the given features.
    
    Same problem as raw2prediction, since it is using it
    ```


Mucho cuidado con caracteres extraños en los nombres de las columnas, importante no usar label como nombre de columna
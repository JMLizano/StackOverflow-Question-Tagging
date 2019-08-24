Serialization

Seems like serialization is still a TODO in Spark, lot of sources advise against using it for custom models. 
https://www.youtube.com/watch?v=n8lIqL8w1fg

There are not a lot of options out there: https://github.com/combust/mleap

MLEAP seems the most intestering one, but you are forced to rewrite the transform() method from the model in plain
Scala. I will go with my own serialization format, and serving using some standar web server

OneVsRest Model

We can't not support the setThreshold at the Onevsrest model, since there is no way to tell if the models have
the setThreshold method (even with the hasThreshold trait) so we leave it to be set at the binary classification
model by the user
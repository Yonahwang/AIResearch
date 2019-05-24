# ************************
# This is a test file
# Auther : fengjiao
# **************************

import tensorflow as tf
import numpy as np

## prepare the original data
with tf.name_scope('data'):
     x_data = np.random.rand(100).astype(np.float32)
     y_data = 0.3*x_data+0.1
##creat parameters
with tf.name_scope('parameters'):
     weight = tf.Variable(tf.random_uniform([1],-1.0,1.0))
     bias = tf.Variable(tf.zeros([1]))
##get y_prediction
with tf.name_scope('y_prediction'):
     y_prediction = weight*x_data+bias
##compute the loss
with tf.name_scope('loss'):
     loss = tf.reduce_mean(tf.square(y_data-y_prediction))
##creat optimizer
optimizer = tf.train.GradientDescentOptimizer(0.5)
#creat train ,minimize the loss 
with tf.name_scope('train'):
     train = optimizer.minimize(loss)
#creat init
with tf.name_scope('init'): 
     init = tf.global_variables_initializer()
##creat a Session 
sess = tf.Session()
##initialize
writer = tf.summary.FileWriter("logs/", sess.graph)
sess.run(init)
## Loop
for step  in  range(101):
    sess.run(train)
    if step %10==0 :
        print step ,'weight:',sess.run(weight),'bias:',sess.run(bias)

        
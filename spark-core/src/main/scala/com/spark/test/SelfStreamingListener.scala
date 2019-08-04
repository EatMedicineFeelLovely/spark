package com.spark.test

import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError

class SelfStreamingListener extends StreamingListener {
  override def onOutputOperationCompleted(
    outputOperationCompleted: StreamingListenerOutputOperationCompleted) {
    outputOperationCompleted.outputOperationInfo.failureReason
  }
  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    
    receiverError.receiverInfo.lastError
    
    
  }
}
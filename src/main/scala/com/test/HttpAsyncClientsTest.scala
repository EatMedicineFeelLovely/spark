/*package com.test

import org.json.JSONObject
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpPost
import java.net.URI
import java.net.URL
import org.apache.http.concurrent.FutureCallback
import org.apache.http.HttpResponse
import java.util.concurrent.CountDownLatch
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.apache.http.nio.client.methods.AsyncCharConsumer
import java.nio.CharBuffer
import org.apache.http.nio.IOControl
import org.apache.http.protocol.HttpContext
import org.apache.http.nio.client.methods.HttpAsyncMethods
import java.util.ArrayList
import org.apache.http.impl.nio.conn.ManagedNHttpClientConnectionFactory
import org.apache.http.params.HttpParams
import org.apache.http.params.BasicHttpParams

*//**
 * http异步发消息
 *//*
object HttpAsyncClientsTest {
  def main(args: Array[String]): Unit = {
   //testHttpClient
   //HttpAsyncClients
   //testURLConnect
    //var sd:ManagedNHttpClientConnectionFactory=new ManagedNHttpClientConnectionFactory
   var a=testURLConnect
   println(a)
   // testHttpClient
  }
  def testHttpClient(){
    var get = new HttpGet()
    val ps=new BasicHttpParams()
     var httpClient = new DefaultHttpClient();
    var id=0
    var latch = new CountDownLatch(10);
    for(i<- 1 to 10){
      id=i
      get.setURI(URI.create(s"https://www.baidum/s?wd=${id}"))
      println(i+":"+get.getURI)
      val rp= httpClient.execute(get);
      println((rp.getStatusLine))
      get.reset()//必须加这个，否则会报错
    
    }
    
   
    
  }
  def testYiBUHttp(){
    val requestConfig = RequestConfig.custom()
				.setSocketTimeout(1).setConnectTimeout(1).build();
		var httpclient = HttpAsyncClients.custom()
				            .setDefaultRequestConfig(requestConfig).build(); 
        httpclient.start();  
        try {  
            val future = httpclient.execute(  
                    HttpAsyncMethods.createGet("https://www.verisign.com/"),  
                    new MyResponseConsumer(), null);  
            if(future!=null){
             future.get
            }
            val result=true
            if (result != null && result.booleanValue()) {  
                System.out.println("Request successfully executed");  
            } else {  
                System.out.println("Request failed");  
            }  
            System.out.println("Shutting down");  
        } finally {  
            httpclient.close();  
        }  
        System.out.println("Done");  
  }
  def testURLConnect()={
    val requestConfig = RequestConfig.custom()
				.setSocketTimeout(10)//连上之后，持续的时间，用于控制返回
				.setConnectTimeout(10)//连上的时候，用于控制ping
				.build();
		var httpclient = HttpAsyncClients.custom()
				            .setDefaultRequestConfig(requestConfig)
				            //.setMaxConnTotal(10000)
		                //.setMaxConnPerRoute(1000)
		                .build();
		var erorURL=new ArrayList[String]
		try {
			httpclient.start();
			var requests = Array[HttpGet](new HttpGet("https://www.google.com.hk"),
					new HttpGet("https://www.verisign.com/"),
					new HttpGet("http://carat.clientsolutions.cn"),
					new HttpGet("http://www.baidu.com/"));
			val latch = new CountDownLatch(requests.length);
			for (request<-requests) {
				httpclient.execute(request, new FutureCallback[HttpResponse]() {
         def completed(response:HttpResponse ) {
                	try {
                	  println("success:"+request.getURI)
                	  latch.countDown();
                	}
                	catch {case t: Throwable => erorURL.add(request.getURI.toString())}
                }
         def failed(ex: Exception ) {
                	try {
                	  println("error:"+request.getURI)
                	  latch.countDown();
                	  erorURL.add(request.getURI.toString())
                	} 
                	catch {case t: Throwable => erorURL.add(request.getURI.toString())}
                }
        def cancelled() {
                	try {latch.countDown();} 
                	catch {case t: Throwable => erorURL.add(request.getURI.toString())}
           }
				});
				
			}
			
			latch.await();
			//System.out.println("Shutting down");
		} finally {
			httpclient.close();
		}
		System.out.println("Done");
		erorURL
  }
  def testSFun(){
    var carat_bidid="a"
    var carat_price="b"
    var str=s"http://carat.clientsolutions.cn/c=1,1,2&bidid=${carat_bidid}&ep=${carat_price}"
    println(str)
  
  }
  def testPinjie(){
    
    var s=new JSONObject
    s.put("key", "Hello Json")
    s.put("key2", Array(1,2,3))
    println(s)
  }

}
class MyFutureCallback(latch:CountDownLatch,request: HttpGet) extends FutureCallback[HttpResponse]{
  //无论完成还是失败都调用countDown()
					@Override
					def completed(response:HttpResponse) {
						latch.countDown();
						System.out.println(response.getStatusLine());
					}
					@Override
					def failed( ex: Exception) {
						latch.countDown();
						System.out.println(request.getRequestLine() + "->" + ex);
					}
					@Override
					def cancelled() {
						latch.countDown();
					}
}
class MyResponseConsumer extends AsyncCharConsumer[Boolean] {  
          
        val times = 0;  
          
        def getTimes()= {  
            "\n\n### 第" + times + "步\n###"
        }  
  
        @Override  
       def onCharReceived(buf: CharBuffer , ioctrl: IOControl ){  
            System.out.println(getTimes() + "onCharReceived");  
            while (buf.hasRemaining()) {  
                System.out.print(buf.get());  
            }  
        }  
        
        @Override
        def onResponseReceived(response: HttpResponse){
          //println(getTimes() + "onResponseReceived");  
        }
        @Override  
        def buildResult(context: HttpContext ) ={  
            System.out.println(getTimes() + "buildResult");  
            true
        }    
  
}  
def doAsyncGet(String url) throws IOException{
RequestConfig defaultRequestConfig = RequestConfig.custom()
				  .setSocketTimeout(5000)
				  .setConnectTimeout(5000)
				  .setConnectionRequestTimeout(5000)
				  .setStaleConnectionCheckEnabled(true)
				  .build();
		final CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom()
		.setDefaultRequestConfig(defaultRequestConfig)
		.setMaxConnTotal(10000)
		.setMaxConnPerRoute(1000).build();
		try {
			final HttpGet httpget = new HttpGet(url);
			RequestConfig requestConfig = RequestConfig.copy(defaultRequestConfig)
				    .build();
			httpget.setConfig(requestConfig);
			httpclient.execute(httpget, new FutureCallback<HttpResponse>() {

                public void completed(final HttpResponse response) {
                	try {
						httpget.releaseConnection();
					} catch (Exception e) {
						log.error("close asyncResponse error:",e);
					}
                }

                public void failed(final Exception ex) {
                	try {
						httpget.releaseConnection();
						log.error("this connection failed!",ex);
					} catch (Exception e) {
						log.error("close asyncResponse error:",e);
					}
                }

                public void cancelled() {
                	try {
						httpget.releaseConnection();
						log.error("this connection has been cancelled!");
					} catch (Exception e) {
						log.error("close asyncResponse error:",e);
					}
                }});
		}catch(Exception e){
			log.error("http async error:"+url,e);
		}
	}

{
					//无论完成还是失败都调用countDown()
					@Override
					def completed(response:HttpResponse) {
						latch.countDown();
						System.out.println(request.getRequestLine() + "->"
								+ response.getStatusLine());
					}
					@Override
					def failed( ex: Exception) {
						latch.countDown();
						System.out.println(request.getRequestLine() + "->" + ex);
					}
					@Override
					def cancelled() {
						latch.countDown();
					}
				}*/
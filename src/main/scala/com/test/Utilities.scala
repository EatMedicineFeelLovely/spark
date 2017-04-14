package com.test

import org.json.JSONObject
import org.json.JSONArray
import org.json.JSONException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern
import java.util.Calendar
import java.lang.Long

trait Utilities {
   def transObject(o1:JSONObject):JSONObject={
        val o2=new JSONObject();
         val it = o1.keys();
            while (it.hasNext()) {
                val key = it.next().asInstanceOf[String];
                val obj = o1.get(key);
                if(obj.getClass().toString().endsWith("String")){
                    o2.accumulate(key.toLowerCase(), obj);
                }else if(obj.getClass().toString().endsWith("JSONObject")){
                    o2.accumulate(key.toLowerCase(), transObject(obj.asInstanceOf[JSONObject]));
                }else if(obj.getClass().toString().endsWith("JSONArray")){
                    o2.put(key.toLowerCase(), transArray(o1.getJSONArray(key)));
                }
            }
            o2
    }
    def transArray( o1:JSONArray):JSONArray={
        val o2 = new JSONArray();
        for (i <- 0 to o1.length-1) {
            val jArray=o1.getJSONObject(i);
            if(jArray.getClass().toString().endsWith("JSONObject")){
                o2.put(transObject(jArray.asInstanceOf[JSONObject]));
            }else if(jArray.getClass().toString().endsWith("JSONArray")){
                o2.put(transArray(jArray.asInstanceOf[JSONArray]));
            }
        }
         o2;
    }
 
  def getNMonthAgo(calendar: Calendar, n: Int) = {
    
    calendar.add(Calendar.MONTH, -n)
    calendar
  }
    
  def getNDayAgo(n: Int) = {
    val calendar = Calendar.getInstance
    val time = calendar.getTimeInMillis - n*24*60*60*1000 
    calendar.setTimeInMillis(time)
    calendar
  }
  
  def getDateStr(calendar: Calendar) = {
    val date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val str = sdf.format(date)
     str
  }
  
  def getDateStr_(calendar: Calendar) = {
    val date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val str = sdf.format(date)
     str
  } 
  
  def getDateStr_(time: Long) = {
    val date = new Date(time)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val str = sdf.format(date)
     str
  }
  
   def getDateStr() = {
    val date = new Date()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val str = sdf.format(date)
     str
  }
  
   def getDateStr_() = {
    val date = new Date()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val str = sdf.format(date)
     str
  }
   
  def getMonthStart() = {
    val cale = Calendar.getInstance()
    cale.add(Calendar.MONTH, 0)
    cale.set(Calendar.DAY_OF_MONTH, 1)
    val firstday = getDateStr_(cale)
    firstday
  }
  
  def getMonthEnd() = {
    val cale = Calendar.getInstance()
    cale.add(Calendar.MONTH, 1)
    cale.set(Calendar.DAY_OF_MONTH, 0)
    val lastday = getDateStr_(cale)
    lastday
  }
  
  def isnull(key: Object): Boolean = {
    if (key != null) {
      true
    } else {
      false
    }
  }

  def getCurrent_time(): Long = {
    val now = new Date()
    val a = now.getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  def getZero_time(): Long = {
    val now = new Date()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val a = dateFormat.parse(dateFormat.format(now)).getTime
    var str = a + ""
    str.substring(0, 10).toLong
  }

  def getTimestamp(): String = {
    var ts = System.currentTimeMillis()
    ts.toString
  }

  def getMD5hash(s: String) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }

  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.WARN)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile("../twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern(): Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
}
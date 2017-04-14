package com.zhiziyun.bot.service.url.test

import org.json.JSONObject
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.json.JSONArray
object JsonTest {
  def main(args: Array[String]): Unit = {
    //test
  val b= """{"DATA":{"MOD_MOB_DDQ_BASIC":[{"AGENT":"wechat","ZODIAC":"兔","STAR":"处女座","GENDER":"FEMALE","EDUCATION_DEGREE":"zkjyx","IS_LOCAL":"bendiji"},{"AGENT":"APP","ZODIAC":"猪","STAR":"双鱼座","GENDER":"MALE","EDUCATION_DEGREE":"bk","IS_LOCAL":"feibendiji"},{"AGENT":"wechat","ZODIAC":"马","STAR":"天秤座","GENDER":"MALE","EDUCATION_DEGREE":"zkjyx","IS_LOCAL":"bendiji"},{"AGENT":"APP","ZODIAC":"鼠","STAR":"摩羯座","GENDER":"MALE","EDUCATION_DEGREE":"bk","IS_LOCAL":"bendiji"}]},"TOPIC":"mod_mob_ddq_basic"}

      """
  val a="""{"qmart":"TEST","ntnum":"50","ecrule1":"测试中1","ecrule2":"","ecrule3":"",}"""
  val obj=new JSONObject(b)
  println(transObject(transObject(obj)))
  }
   def transObject(o1:JSONObject):JSONObject={
        val o2=new JSONObject();
         val it = o1.keys();
            while (it.hasNext()) {
                val key = it.next().asInstanceOf[String];
                val obj = o1.get(key);
                if(obj.getClass().toString().endsWith("String")){
                    o2.accumulate(key.toUpperCase(), obj);
                }else if(obj.getClass().toString().endsWith("JSONObject")){
                    o2.accumulate(key.toUpperCase(), transObject(obj.asInstanceOf[JSONObject]));
                }else if(obj.getClass().toString().endsWith("JSONArray")){
                    o2.put(key.toUpperCase(), transArray(o1.getJSONArray(key)));
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
}
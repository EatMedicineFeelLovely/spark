package com.test

import java.io.File
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter

object ImageTes {
  def main(args: Array[String]): Unit = {
    val parentPath="C:\\Users\\zhiziyun\\Desktop\\ml\\mnist_data"
    val outPath="C:\\Users\\zhiziyun\\Desktop\\ml\\jpgint.txt"
    val file=new File(parentPath)
    val writer = new PrintWriter(new File(outPath))
    file.listFiles().foreach { x => 
      val name=x.getName
      val mark = name.split("\\.")(0)
      val path=x.getPath
      t(path,mark,writer)
      }
    writer.flush()
    writer.close()
    
  }

  def t(path: String, mark: String,writer: PrintWriter) {
    val file = new File(path);
    val image = ImageIO.read(file);
    val arr = new ArrayBuffer[Int](28 * 28 + 1)
    arr.+=(mark.toInt)
    for (i <- 0 to image.getHeight - 1) {
      for (j <- 0 to image.getWidth - 1) {
        val pixel = (image.getRGB(j, i))
        val r = (pixel & 0xff0000) >> 16; //r
        val g = (pixel & 0xff00) >> 8; //g
        val b = (pixel & 0xff); //b
        if (r >= 130) {
          //print("1 ")
          arr.+=(1)
        } else {
          //print("  ")
          arr.+=(0)
        }
      }
      println()
    }
    writer.write(arr.mkString(",")+"\n")
  }

}
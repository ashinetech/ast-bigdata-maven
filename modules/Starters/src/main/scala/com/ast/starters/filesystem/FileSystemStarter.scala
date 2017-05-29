package com.ast.starters.filesystem

/**
  * Created by cloudera on 5/28/17.
  */
import java.io._

import scala.io.Source

object FileSystemStarter {

  def main(args: Array[String]) {

    //Writing to a new file
    val writer = new PrintWriter(new File("/home/cloudera/NAS/test.txt" ))

    writer.write("Hello Scala!!\n")
    writer.close()

    println("File write completed!")

    //File Read
    println("Following is the content read:" )

    Source.fromFile("/home/cloudera/NAS/test.txt" ).foreach {
      print
    }
  }
}

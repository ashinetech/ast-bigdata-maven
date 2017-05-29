package com.ast.starters.mysql

/**
  * Created by cloudera on 5/28/17.
  */

import java.sql.{Connection, DriverManager}

/**
  * A Scala JDBC connection starter example.
  */
object MySQLStarter {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/retail_db"
    val username = "root"
    val password = "cloudera"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * from categories")
      while ( resultSet.next() ) {
        val category_name = resultSet.getString("category_name")
        //val user = resultSet.getString("user")
        println("category_name = " + category_name )
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }

}

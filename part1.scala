package com.challenge.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.util.{Try,Success,Failure}

object part1 {
  
  def parseLine(line: String) : Option[(String, (String, String, Int))] = {
    // Função parser para separar cada linha para uma tupla = (HOST, (DATE, REQUEST, BYTES))
    val fields = line.split(" - - \\[")
    val fields2 = if(fields.length > 1){
        fields(0).trim().split("\\s+")
      }else{
        println("\nFormato inválido na linha: \"" + line + "\"")
        return None
    }
    
    val HOST = fields2(fields2.length - 1 ).trim()
    val DATE = fields(1).split(":")(0).trim()
    
    val fields3 = fields(1).split("\\s+")
    val REQUEST = fields3(fields3.length - 2).trim()
       
    var tmp = 0
    try{
      //
      //Para alguns casos de quantidade de Total de bytes retornados foi identificado o char "-" e será substituido por 0
      //nos outros casos será convertido para Int
      tmp = if(fields3(fields3.length - 1).trim() == "-") 0 else fields3(fields3.length - 1).trim().toInt
    } catch {
      case e: Exception => println("\nErro: " + e.toString + "\nna linha: \"" + line + "\"")
    }
    
    val BYTES = tmp
    return Some((HOST, (DATE, REQUEST, BYTES)))
  }
  
  def main(args: Array[String]) {
    // Somente mostrar erros
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Cria um SparkContext com todos os nós da máquina local
    val sc = new SparkContext("local[*]", "part1")
    
    // Cria um RDD com todas as linhas do arquivo
    val LinesJul = sc.textFile("access_log_Jul95")
    val LinesAug = sc.textFile("access_log_Aug95")
    
    // Identifica a tupla (HOST, (DATE, REQUEST, BYTES)) em cada linha
    val TuplasJul = LinesJul.flatMap(parseLine)
    val TuplasAug = LinesAug.flatMap(parseLine)
    
    // Executa a action UNION nos dois RDDs e armazena em cache do RDD retornado
    val Tuplas = TuplasJul.union(TuplasAug).persist()
    
    // Cálculo do número de hosts únicos
    val UniqueHosts = Tuplas.groupByKey().map(x => (x._1,1)).count()
    println(s"\n1) Número total de hosts únicos foram: $UniqueHosts")
    
    // Filtra o RDD com somente linhas que sejam de erros 404 e deixa ele em cache para melhora de performance
    val Erros404 = Tuplas.filter(x => x._2._2 == "404").persist()
    
    // Cálculo do total de erros 404, para isso será criado um RDD com a tupla (REQUEST, 1) e agregados
    val AmountErros404 = Erros404.map( x => (x._2._2, 1)).count()
    println(s"\n2) Número total de erros 404 no período foram: $AmountErros404")
    
    // Determinar as 5 URLs que mais causaram erros 404, para isso será criado um RDD com a tupla (HOST, 1), uma linha para cada erro 404
    // O RDD resultante será agregado pela chave (host) e ordenado
    val HostErros404 = Erros404.map( x => (x._1, 1)).reduceByKey( (x,y) => x + y )
    val Hosts = HostErros404.map( x => (x._2, x._1) ).sortByKey(false).take(5)
    println("\n3) Os 5 URLs que mais causaram erros 404 foram:\n")
    for (h <- Hosts) {
      var host = h._2
      var req = h._1
       println(s"$host: $req requisições") 
    }
    
    // Quantidade de erros 404 por dia, para isso será criado um RDD com a tupla (DATA, 1) e agregados
    val Errors404PerDay = Erros404.map( x => (x._2._1, 1)).reduceByKey( (x,y) => x + y)
    println(s"\n4) A quantidade de erros 404 por dia foi:\n")
    Errors404PerDay.foreach(println)
    
    // Total de bytes retornados.
    val ReturnedBytes = Tuplas.map(x => x._2._3).sum()
    val FormattedReturnedBytes = f"$ReturnedBytes%.0f"
    println(s"\n5) O total de bytes retornados foi de: $FormattedReturnedBytes bytes.")
    
  }
}
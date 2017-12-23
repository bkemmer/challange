package com.challenge.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

/** Cálculo da métrica de Centralidade de Proximidade
 *  Distanciamento e Proximidade 
 *  para isso será utilizado o algoritmo Breath for Search - BFS*/

object part2 {
  // Definição do tipo de dado BFSData que contém um vetor de strings referentes a suas conexões, sua distancia e uma flag:
  // 0 - Vértice ainda não processado
  // 1 - Vértice em análise
  // 2 - Vértice já analisado
  type BFSData = (Array[String], Int, Byte)
  
  // Definição do tipo de dado BFSNode que contém o vertice e seu BFSData
  type BFSNode = (String, BFSData)
  
  // Definição do tipo de dado Metricas = (VerticeID, Distanciamento, Proximidade)
  type Metricas = (String, Int, Double)
  
  def parseLine(line: String) : Option[(String, String)] = {
    // Função parser para separar cada linha para uma tupla = (String, String)
    val fields = line.split("\\s+")
    var A = ""
    var B = ""
    if (fields.length > 1){
      A = fields(0).trim()
      B = fields(1).trim()
    }else {
      println("\nErro na linha: " + line)
      return None
    }
    return Some((A, B))
  }
  
  /** Converte uma linha em um BFSNode */
  def conversor( line: (String, Array[String]), VerticeInicial: String): BFSNode = {
    //Recebe a chave que representa o verticeID a ser analisado
    var verticeID = line._1
    
    // Seta os valores default para a distancia e flag para o estado 0
    var flag:Byte = 0
    var distancia:Int = 2147483647 //Representando uma distancia inicial infinita
    
    //Caso o vertice atual seja igual ao vertice em que se está sendo feita a análise da rede
    if (verticeID == VerticeInicial){
      flag = 1
      distancia = 0
    }
    return (verticeID, (line._2, distancia, flag))
  }
  /** Mapeia o vertice atual e seus Vertices diretos */
  def verticesDiretos(vertice:BFSNode): Array[BFSNode] = {
    // Obtém os dados do vertice que está sendo analisado
    val verticeID = vertice._1
    val data:BFSData = vertice._2
    val vDiretos:Array[String] = data._1
    val distancia:Int = data._2
    var flag:Byte = data._3
    
    // Vetor de retorno com todos os Vertices com conexões diretas encontrados
    var resultados:ArrayBuffer[BFSNode] = ArrayBuffer()
    
    // Vertices com conexões diretas são marcados com flag = 1, para que sejam analisados
    if(flag == 1){
      // para cada elemento do vetor de vertices com conexões diretas será analisada a distancia
      for(v <- vDiretos){
        val vID:String = v
        // adiciona +1 a distancia do Vertice atual
        val vDistancia:Int = distancia + 1
        val vFlag:Byte = 1
      
      // Cria um novo elemento nos vertices já analisados e adiciona ao vetor de resultados
      // duplicando os vertices no vetor de resultados porém seram redutor "deduplicados"
      val newVertice:BFSNode = (vID, (Array(), vDistancia, vFlag))
      resultados += newVertice
      }
      // Muda o flag para 2, marcando o vértice como processado
      flag = 2
    }
    // Adiciona o Vértice para ser processado pelo redutor
    val verticeAtual:BFSNode = (verticeID, (vDiretos, distancia, flag))
    resultados += verticeAtual
    return resultados.toArray
  }
  /* Unifica todos os vertices com mesmo verticeID, restando somente o que tiver a menor distancia e o maior flag*/
  def redutor(x:BFSData, y:BFSData): BFSData ={
    // Extrai os dados de input que serão unificados
    // BFSData = (Array[String], Int, Byte)
    val V1:Array[String] = x._1
    val V2:Array[String] = y._1
    val dist1:Int = x._2
    val dist2:Int = y._2
    val flag1:Byte = x._3
    val flag2:Byte = y._3
    
    // Preserva as conexões do vertice original já que somente ele terá conexões
    var vertices:ArrayBuffer[String] = ArrayBuffer()
    if(V1.length > 0 ) vertices ++= V1
    if(V2.length > 0 ) vertices ++= V2
    // Presever somente o maior flag e a menor distância
    val flag = if (flag1 >= flag2) flag1 else flag2
    val distancia = if (dist1 <= dist2) dist1 else dist2
    return (vertices.toArray, distancia, flag)
  }
  
  def main(args: Array[String]) {
    // Somente mostrar erros
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Cria um SparkContext com todos os processadores da máquina local
    val sc = new SparkContext("local[*]", "part2")
    
    // Cria um RDD com todas as linhas do arquivo
    val LinesEdges = sc.textFile("edges")
    
    // Identifica a tupla (A, B) em cada linha, ou seja um a conexão entre os dois vértices
    val TuplasA_B = LinesEdges.flatMap(x => parseLine(x))

    // Executa a UNION de (A, B) e (B,A), e retornando um RDD com apenas tuplas distintas
    val Tuplas = TuplasA_B.union(TuplasA_B.map(x => (x._2, x._1))).distinct()
   
    // Agrupa uma linha para cada vertice e uma lista com suas conexões 
    val TuplasPerKey = Tuplas.groupByKey().map(x => (x._1.toString, x._2.toArray))
    
    // Cria uma lista com todos os vertices do grafo
    val VerticesUnicos = TuplasPerKey.map(x => x._1).collect().toList
    
    // Inicializa as variáveis que terão os resultados
    var resultadoMetricas:ArrayBuffer[Metricas] = ArrayBuffer()
    var distanciamento:Int = 0
    var proximidade:Double = 0
    var metrica:Metricas = ("",0,0)
    
    // Percorre a lista de todos os vértices únicos e executa o mapeamento para cada vértice 
    for (v <- VerticesUnicos){
      
      // Converte o Vértice e suas conexões para um formato BFSNode 
      var TuplasBFS = TuplasPerKey.map( x => conversor(x, v))
      do {
        // Executa a redução das linhas duplicadas
        TuplasBFS = TuplasBFS.flatMap(verticesDiretos).reduceByKey(redutor)
      } while (!(TuplasBFS.filter(x => x._2._3 < 2).take(1).isEmpty)) 
        // Será executado enquanto existir algum Vertice com flag < 2
        // ou seja que ainda precisa ser analisada
      
      distanciamento = TuplasBFS.map(x => x._2._2).sum().toInt
      proximidade = 1 / distanciamento.toDouble
      metrica = (v,distanciamento,proximidade)
      
      resultadoMetricas += metrica
    }
    
    //Ordena e retorna os resultados ordenado pela proximidade de forma ascendente
    resultadoMetricas.sortBy(x=> x._3).foreach(println)
  }
}
package com.spark.fraud
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut

object GraphX {
  def main(args: Array[String]): Unit = {
    /**the basic graph class in GraphX is called Graph, which contains
      two RDDs: one for edges and one for vertices
      VertexRDD === RDD[(VertexId, VD)]
      EdgeRDD   === RDD[Edge[ED]] ---> Edge[Attr] === Edge(Src, Des, Attr)
      VD and ED serve as placeholders for user-defined classes
      VertexID are 64-bits longs
     */
    val conf = new SparkConf().setMaster("local").setAppName("GraphX apps")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"),(4L, "Diane"), (5L, "went to the gym this morning")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")
    ))
    val myGraph = Graph(myVertices, myEdges)
    myGraph.vertices.collect
    myGraph.edges.collect
    //join vertices and edges together
    myGraph.triplets.collect //returns an RDD of EdgeTriplet[VD, ED] subclass of Edge[RDD]
    /**EdgeTriplets provide key fields:
      Attr
      srcId
      srcAttr
      dsId
      dsAttr
     */
    myGraph.triplets.map(triplet => triplet.srcAttr).foreach(println(_))
    //mapTriplets
    myGraph.mapTriplets(t => (t.attr, t.attr=="is-friends-with" &&
      t.srcAttr.toLowerCase.contains("a"))).triplets.collect.foreach(println) //this new graph isn't captured in any variable
    //aggregateMessages() to count out-degree of each vertex
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).collect.foreach(println)
    /** def aggregateMessages[Msg] (
            sendMsg: EdgeContext[VD, ED, Msg] => Unit
            mergeMsg: (Msg, Msg) => Msg) : VertexRDD[Msg]
     */
    //match up VertexIds with vertex data
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).
       join(myGraph.vertices).collect.foreach(println)
    //select only the second Tuple2 and then sway the orders
     myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).
      join(myGraph.vertices).map(_._2.swap).collect.foreach(println)
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).
      rightOuterJoin(myGraph.vertices).map(_._2.swap).collect.foreach(println)
    //using Options[]'s getOrElse()
    myGraph.aggregateMessages[Int](_.sendToSrc(1), _ + _).
      rightOuterJoin(myGraph.vertices).
      map(x => (x._2._2, x._2._1.getOrElse(0))).collect.foreach(println)

   // function that compute distance to the furthest ancestor for each vertex (MUST BE REVISITED)

    def sendMsg(ec: EdgeContext[Int, String, Int]): Unit = {
       ec.sendToDst(ec.srcAttr + 1)
     }

  def mergeMsg(a: Int, b: Int) = {
    math.max(a, b)
  }

    def propagateEdgeCount(g: Graph[Int, String]) : Graph[Int, String] = {
      val verts = g.aggregateMessages[Int](sendMsg, mergeMsg)
      val g2 = Graph(verts, g.edges)
      val check = g2.vertices.join(g.vertices).
        map( x => x._2._1 - x._2._2).
        reduce(_ + _)
      if (check > 0)
        propagateEdgeCount(g2)
      else
        g
    }
    val initialGraph = myGraph.mapVertices((_,_) => 0)

    initialGraph.vertices.collect.foreach(println)
    propagateEdgeCount(initialGraph).vertices.collect.foreach(println)

    /**Serialization/deserialization
     * Reading/writing binary format: we are going to read/write a standard
     * Hadoop sequence file which is a binary file containing a sequence of
     * serialized objects. The Spark RDD API function saveAsObjectFile() saves
     * to Hadoop sequence file
     * to save to HDFS instead use: "hdfs://localhost:8020/myGraphVertices"
     */
    /* fails due to case insensitivity
    myGraph.vertices.saveAsObjectFile("src/mygraphvertices")
    myGraph.edges.saveAsObjectFile("src/mygraphedges ")
    val myGraph2 = Graph(sc.objectFile[Tuple2[VertexId, String]]("src/mygraphvertices"),
      sc.objectFile[Edge[String]]("src/mygraphedges"))
     */

    /**performing way to serialize/deserialize to/from JSON
     * when using a distributed filesystem coalesce(1, true) must be eliminated !!!!!
     * example of when to use coalesce/reparation:
     * https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
     */
  /*  myGraph.vertices.mapPartitions(vertices => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      vertices.map(v => {writer.getBuffer.setLength(0)
                          mapper.writeValue(writer, v)
                          writer.toString})
    }).coalesce(1, true).saveAsTextFile("myJsonGraphVertices")

    myGraph.edges.mapPartitions(edges => {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val writer = new java.io.StringWriter()
      edges.map(e => {writer.getBuffer.setLength(0)
                      mapper.writeValue(writer, e)
                      writer.toString})
    }).coalesce(1, true).saveAsTextFile("myJsonGraphEdges")

    val myGraph2 = Graph(
      sc.textFile("myJsonGraphVertices").mapPartitions(vertices => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        vertices.map(v => {
          val r = mapper.readValue[Tuple2[Integer, String]](v,
            new TypeReference[Tuple2[Integer, String]] {})
          (r._1.toLong, r._2)
        })
      }),
      sc.textFile("myJsonGraphEdges").mapPartitions(edges => {
        val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        mapper.registerModule(DefaultScalaModule)
        edges.map(e => mapper.readValue[Edge[String]](e,
          new TypeReference[Edge[String]] {}))
        })
    )

*/


  /** pregel method signature
   * def pregel[A]
   *      (initialMsg: A,
   *       maxIter: Int = Int.MaxValue
   *       activeDir: EdgeDirection = EdgeDirection.out)
   *       (vprog: (VertexId, VD, A) => VD,
   *        sendMsg: EdgeTriplet[VD, ED] => Iterator[(Vertex, A)],
   *        mergeMsg: (A, A) => A)
   *      : Graph[VD, ED]
   */
  val g = Pregel(myGraph.mapVertices((vid, vd) => 0), 0,
                  activeDirection = EdgeDirection.Out)(
                  (id:VertexId, vd:Int, a:Int) => math.max(vd,a),
                  (et: EdgeTriplet[Int, String]) =>
                    Iterator((et.dstId, et.srcAttr+ 1 )),
                  (a: Int, b:Int) => math.max(a,b))


/**
 * Run personalized PageRank for a given vertex, such that all random walks
 * are started relative to the source node.
 * ----------> Topic-specific (personalized) PageRank
 */

    val arvix = GraphLoader.edgeListFile(sc, "src/cit-HepTh.txt")
    val a = arvix.personalizedPageRank(9207016, 0.001)
    .vertices.filter(_._1 != 9207016).reduce((a, b) => if (a._2 > b._2) a else b)
    println(a)

    /**
     * dynamic pagerank
     */
   arvix.pageRank(0.0001).vertices.sortBy(_._2).foreach(println)
    /** on trouve que le papier le plus "important" est
     * "Noncompact Symmetries in String Theory":
     * https://arxiv.org/pdf/hep-th/9207016.pdf
     */
   // arvix.pageRank(0.0001).edges.foreach(println)
  //  myGraph.pageRank(0.001).vertices.foreach(println)
  //  myGraph.pageRank(0.001).edges.foreach(println)

    val someVertices = sc.makeRDD(Array((1L, "A") ,(2L, "B"), (3L, "C")))
    val someEdges = sc.makeRDD(Array(Edge(1L, 2L, "12"), Edge(2L, 3L, "23"), Edge(3L, 1L, "31"),
      Edge(1L, 3L, "13")))
    val someGraph = Graph(someVertices, someEdges)
   // someGraph.pageRank(0.001).vertices.collect.foreach(println)
    //someGraph.pageRank(0.001).edges.collect.foreach(println)
    val slashGraph = GraphLoader.edgeListFile(sc, "src/Slashdot0811.txt").cache
    val slashGraph2 = Graph(slashGraph.vertices, slashGraph.edges.map(e =>
    if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr))).
      partitionBy(PartitionStrategy.RandomVertexCut)
    (0 to 6).map(i => slashGraph2.subgraph(vpred = (vid, _) => vid >=i*10000 && vid < (i+1)*10000).triangleCount.
    vertices.map(_._2).reduce(_ + _)) .foreach(println)
    val liste = List(1, 2, 3)
    val seq = Seq("hi", "ha")



  }
}

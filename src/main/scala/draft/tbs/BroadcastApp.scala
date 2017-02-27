package draft.tbs

import java.net.InetSocketAddress

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{RunnableGraph, Sink, Source}
import io.netty.channel.nio.NioEventLoopGroup


object BroadcastApp {

  // todo proper config file + akka cfg
  val MaxTickers = 1024
  val MaxSourceSize = 4096
  val DownstreamPort = 5556

  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("BroadcastApp")
    implicit val materializer = ActorMaterializer()


    val bossGroup = new NioEventLoopGroup(1)
    val wrkGroup = new NioEventLoopGroup()

    val source: Source[CandleData, ActorRef] = Source.actorRef[QuoteData](MaxSourceSize, OverflowStrategy.dropTail)
      .groupBy(MaxTickers, _.ticker)
      .via(QuoteAggregator.minutes(1))
      .mergeSubstreams

    val downstreamServer = new DownstreamServer(bossGroup, wrkGroup, system, DownstreamPort)
    val g: RunnableGraph[ActorRef] =  source.to(Sink.actorRef(downstreamServer.actorRef, onCompleteMessage = None))

    val srcActor = g.run()
    val upstreamClient = new UpstreamClient(wrkGroup,
      new InetSocketAddress("localhost", 5555),  { qd ⇒
        //println(s"quote: $qd")
        srcActor ! qd
      })

    downstreamServer.start()
    upstreamClient.start()
    
  }

}

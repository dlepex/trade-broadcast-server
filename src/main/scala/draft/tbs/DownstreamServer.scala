package draft.tbs


import java.io.OutputStream

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, ByteBufOutputStream}
import io.netty.channel._
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.handler.codec.MessageToByteEncoder

import scala.collection.mutable.ArrayBuffer
import scala.util.Random



class DownstreamServer(bossPool: EventLoopGroup,
                       workerPool: EventLoopGroup,
                       system: ActorSystem,
                       port: Int) extends StrictLogging {


  /**
    * Actor that consumes CandleData and sends em to downstream connections.
    */
  val actorRef: ActorRef = system.actorOf(Props[DownstreamServer.Broadcaster])


  def start(): Unit = {
    // todo check start once
    val b = new ServerBootstrap();
    b.group(bossPool, workerPool)
      .channel(classOf[NioServerSocketChannel])
      //.option(ChannelOption.SO_BACKLOG, 100)
      .childHandler(new ChannelInitializer[NioSocketChannel]() {
      @Override
      def initChannel(ch: NioSocketChannel): Unit = {
        val p = ch.pipeline()
        p.addLast(new DownstreamServer.Handler(actorRef))
        p.addLast(new DownstreamServer.JsonEncoder())
      }
    })

    val ch = b.bind(port).sync().channel()
    logger.debug(s"DownstreamServer started on port=$port")
  }


  def stop(): Unit = {
    //todo
  }

}


object DownstreamServer {

  private class Broadcaster extends Actor with ActorLogging {
    import Broadcaster._

    import context.system
    import context.dispatcher
    import scala.concurrent.duration._
    import collection.mutable

    system.scheduler.schedule(60.second, 60.second, self, Shuffle)

    private val channels = ArrayBuffer[Channel]() // ArrayBuffer has no in-place shuffle
    private val rng = new Random
    private val history = mutable.Map[String, Vector[CandleData]]().withDefault(_ ⇒ Vector())


    override def receive = {
      case candle: CandleData ⇒
        var last = history(candle.ticker)
        if (last.size == 10) {
          last = last.drop(1)
        }
        history(candle.ticker) = last :+ candle
        for(ch <- channels) {
          ch.writeAndFlush(candle)
        }
      case Connect(chan) ⇒
        log.debug(s"connect chan $chan")
        channels += chan
        chan.writeAndFlush(history.values.toList.view.flatten)
      case Disconnect(chan) ⇒
        log.debug(s"discon chan $chan")
        channels -= chan // todo optimize O(n)
      case Shuffle() ⇒
        // Shuffling (rarely done) to minimize the chance that the earlier connections always get quote data sooner than the later ones
        shuffleInplace(rng, channels)
    }
  }

  private object Broadcaster {
    final case class Connect(chan: Channel)
    final case class Disconnect(chan: Channel)
    final case class Shuffle()
  }


  private final class Handler(broadcaster: ActorRef) extends ChannelInboundHandlerAdapter with StrictLogging {

    override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
      msg.asInstanceOf[ByteBuf].release()
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      logger.debug("exceptionCaught", cause)
      ctx.close()
    }

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      broadcaster ! Broadcaster.Connect(ctx.channel)
      super.channelActive(ctx)
    }

    override def channelInactive(ctx: ChannelHandlerContext): Unit = {
      broadcaster ! Broadcaster.Disconnect(ctx.channel)
      super.channelInactive(ctx)
    }

  }

  /**
    * This JsonEncoder will "unwrap" iterables/iterators
    */
  private class JsonEncoder extends MessageToByteEncoder[AnyRef] {
    override def encode(ctx: ChannelHandlerContext, msg: AnyRef, out: ByteBuf): Unit = {
      val os: OutputStream = new ByteBufOutputStream(out)
      iter(msg) foreach { m ⇒
        jsonMapper.writeValue(os, m)
        os.write('\n')
      }
    }

    private def iter(msg: AnyRef): Iterator[AnyRef] = msg match {
      case it:Iterator[AnyRef] ⇒ it
      case it:Iterable[AnyRef] ⇒ it.iterator
      case _ ⇒ Iterator(msg)
    }
  }


  private val jsonMapper: ObjectMapper = initMapper()

  private def initMapper() = {

    import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
    import com.fasterxml.jackson.module.scala.DefaultScalaModule

    val m = new ObjectMapper()

    m.registerModule(DefaultScalaModule)
    m.setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
    m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    m
  }


  // in-place copy of Random.shuffle
  def shuffleInplace[T](r: Random, buf: ArrayBuffer[T]): Unit = {
    def swap(i1: Int, i2: Int) {
      val tmp = buf(i1)
      buf(i1) = buf(i2)
      buf(i2) = tmp
    }

    for (n <- buf.length to 2 by -1) {
      val k = r.nextInt(n)
      swap(n - 1, k)
    }
  }


}
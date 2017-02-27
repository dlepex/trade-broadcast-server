package draft.tbs

import java.net.SocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.StrictLogging
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.util.CharsetUtil

import scala.concurrent.duration._

class UpstreamClient(workerPool: EventLoopGroup,
                     remoteAddr: SocketAddress,
                     listener: UpstreamClient.Listener,
                     reconnectWithin: Duration = 1.second,
                     readTimeout: Duration = 5.second) extends  StrictLogging {

  private val startOnce = new AtomicBoolean()
  private var attempt: Long = _
  private val boot = new Bootstrap()

  boot.group(workerPool)
  boot.channel(classOf[NioSocketChannel])
  boot.option(ChannelOption.SO_KEEPALIVE, java.lang.Boolean.TRUE)

  boot.handler(new ChannelInitializer[SocketChannel] {
    def initChannel(chan: SocketChannel): Unit = {
      chan.pipeline().addLast(
        new ReadTimeoutHandler(readTimeout.toMillis, TimeUnit.MILLISECONDS),
        new LengthFieldBasedFrameDecoder(UpstreamClient.MaxQuoteDataLen, 0, 2),
        new UpstreamClient.UnpackingHandler(listener))
    }
  })



  /**
    * Connects to upstream and re-connects if connection was closed => it survives upstream restarts.
    * Call this method once!
    */
  def start(): Unit = {
    if (!startOnce.compareAndSet(false, true)) {
      throw new IllegalStateException(s"Already started for addr=$remoteAddr")
    }
    connectReliably()
  }


  private def connectReliably() {
    attempt += 1
    logger.debug(s"(re)connect to $remoteAddr attempt=$attempt")
    boot.connect(remoteAddr).addListener(new ChannelFutureListener {
      override def operationComplete(fut: ChannelFuture) = {
        if (fut.isSuccess) {
          fut.channel().closeFuture().addListener((_: ChannelFuture) ⇒ {
            connectReliably()
          })
        } else {
          workerPool.schedule(new Runnable {
            def run() = connectReliably()
          }, reconnectWithin.toMillis, TimeUnit.MILLISECONDS)
        }
      }
    })
  }

  def stop(): Unit = {
    //todo
  }
}


object UpstreamClient {
  val MaxQuoteDataLen = 64

  type Listener = QuoteData ⇒ Unit

  def unpack(buf: ByteBuf): Option[QuoteData] = {
    try {
      buf.readShort() // read length prefix
      val timestamp = buf.readLong()
      val tickerLen = buf.readShort()
      if (tickerLen <= 0) {
        return None
      }
      val ticker = buf.readCharSequence(tickerLen, CharsetUtil.UTF_8)
      val price = buf.readDouble()
      val size = buf.readInt()

      if (price < 0 || size < 0) None
      else Some(QuoteData(ticker.toString, timestamp, price, size))
    } catch {
      case _: IndexOutOfBoundsException ⇒ None
    }
  }

  private final class UnpackingHandler(listener: Listener) extends SimpleChannelInboundHandler[ByteBuf] with StrictLogging {
    override def channelRead0(ctx: ChannelHandlerContext, buf: ByteBuf): Unit = {
      // (!) Note that buf is released in SimpleChannelInboundHandler
      unpack(buf) foreach listener
    }
  }
}

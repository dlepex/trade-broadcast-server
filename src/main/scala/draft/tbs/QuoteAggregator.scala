package draft.tbs

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogicWithLogging, InHandler, OutHandler}

class QuoteAggregator[P](periodFn: QuoteAggregator.PeriodFn[P]) extends GraphStage[FlowShape[QuoteData, CandleData]] {

  val in = Inlet[QuoteData]("qagr.in")
  val out = Outlet[CandleData]("qagr.out")

  override val shape = FlowShape.of(in, out)


  override def createLogic(attr: Attributes): GraphStageLogicWithLogging =
    new GraphStageLogicWithLogging(shape) {

      private var currentPeriod: P = periodFn.minimum
      private var open: Double = _
      private var close: Double = _
      private var high: Double = _
      private var low: Double = _
      private var volume: Long = _
      private var ticker: String = _

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val ord = periodFn.ordering
          def beginNewPeriod(q: QuoteData, p: P): Unit = {
            open = q.price; close = open; high = q.price; low = high
            volume = q.size
            currentPeriod = p
          }

          val q = grab(in)
          val period = periodFn(q.timestamp)
          if (ord.lt(period, currentPeriod)) {
            log.debug(s"rogue quote data $q")
            pull(in)
            return
          }
          if (ticker == null) {
            // initial period
            beginNewPeriod(q, period)
            ticker = q.ticker
            pull(in)
          } else if (period == currentPeriod) {
            assert(q.ticker == ticker)
            close = q.price
            low = low min q.price
            high = high max q.price
            volume += q.size
            pull(in)
          } else {
            assert(ord.gt(period, currentPeriod))
            assert(q.ticker == ticker)
            val candle = CandleData(ticker, periodFn.timestampOf(currentPeriod), high, low, open, close, volume)
            beginNewPeriod(q, period)
            push(out, candle)
          }
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

}


object QuoteAggregator {

  /** This is over-engineered something.
    * @tparam P period values
    */
  trait PeriodFn[P] extends (Long ⇒ P) {
    def apply(timestamp: Long): P
    def minimum: P
    def timestampOf(period: P): Long
    def ordering: Ordering[P]
  }

  def seconds(n: Int): QuoteAggregator[Long] = new QuoteAggregator(SecondBasedPeriodFn(n))
  def minutes(n: Int): QuoteAggregator[Long] = seconds(n * 60)

  private final case class SecondBasedPeriodFn(numberOfSeconds: Int) extends PeriodFn[Long] {
    private  val durationInMs: Long = numberOfSeconds * 1000
    override def apply(timestamp: Long): Long = timestamp / durationInMs
    override def minimum: Long = 0
    override def timestampOf(period: Long): Long = period * durationInMs
    override def ordering: Ordering[Long] = Ordering.Long
  }

}

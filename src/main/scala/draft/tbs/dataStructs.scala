package draft.tbs

import java.time.Instant

final case class QuoteData(ticker: String, timestamp: Long, price: Double, size: Int)

final case class CandleData(ticker: String, timestamp: Long, high: Double, low: Double, open: Double, close: Double, volume: Long) {
  // getter for Jackson which shadows timestamp val
  def getTimestamp: String = Instant.ofEpochMilli(timestamp).toString
}
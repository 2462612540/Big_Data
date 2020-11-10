package chapter17.observer.observepattern

//气象局的公告板
class CurrentConditions extends ObServer {

  private var mTemperature: Float = _
  private var mPressure: Float = _
  private var mHumidity: Float = _

  override def update(mTemperature: Float, mPressure: Float, mHumidity: Float) = {
    //更新天气公告板
    this.mTemperature = mTemperature
    this.mPressure = mPressure
    this.mHumidity = mHumidity
    //显示
    display()
  }

  def display() = {
    println("***气象局的天气公告板 Today mTemperature: " + mTemperature + "***")
    println("***气象局的天气公告板 Today mPressure: " + mPressure + "***")
    println("***气象局的天气公告板 Today mHumidity: " + mHumidity + "***")
  }
}

package chapter17.observer.localinternetobserver

//这个类你可以理解成是气象局维护的一个提供天气情况数据的核心类

class WeatherData {

  private var mTemperatrue: Float = _ //温度
  private var mPressure: Float = _ //气压
  private var mHumidity: Float = _ //湿度
  //气象局的天气公告板
  private var mCurrentConditions: CurrentConditions = _
  //腾讯的天气公告板
  private var qqCurrentConditions: CurrentConditions = _

  //构建WeatherData对象时，将mCurrentConditions传入
  def this(mCurrentConditions: CurrentConditions) {
    this
    this.mCurrentConditions = mCurrentConditions
    this.qqCurrentConditions = qqCurrentConditions
  }

  //设置最新的天气情况的方法
  def setData(mTemperature: Float, mPressure: Float, mHumidity: Float) = {
    this.mTemperatrue = mTemperature
    this.mPressure = mPressure
    this.mHumidity = mHumidity
    //要求修改公告板的最新数据
    dataChange()
  }

  //将最新数据的update给mCurrentConditions
  def dataChange() = {

    mCurrentConditions.update(getTemperature(), getPressure(), getHumidity())
    qqCurrentConditions.update(getTemperature(), getPressure(), getHumidity())
  }

  def getTemperature() = {
    mTemperatrue
  }

  def getPressure() = {
    mPressure
  }

  def getHumidity() = {
    mHumidity
  }
}

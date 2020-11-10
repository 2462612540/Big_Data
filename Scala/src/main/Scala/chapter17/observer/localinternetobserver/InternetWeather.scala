package chapter17.observer.localinternetobserver

object InternetWeather {

  def main(args: Array[String]): Unit = {
    //创建个月气象局的天气公告板
    val mCurrentConditions = new CurrentConditions()
    //创建mWeatherData,并将mCurrentConditions设置
    val mWeatherData = new WeatherData(mCurrentConditions)
    //比如天气情况变化，这里设置最新数据
    mWeatherData.setData(30, 150, 40)
  }
}

package chapter08.homework

object BankAccountDemo {
  def main(args: Array[String]): Unit = {
    //测试一把
    //    val checkingAccount = new CheckingAccount(100)
    //    checkingAccount.query()
    //    checkingAccount.withdraw(10) // 手续费1
    //    checkingAccount.query() // 89
    //    checkingAccount.deposit(10) // 手续费1
    //    checkingAccount.query()

    val savingsAccount = new SavingsAccount(100)
    savingsAccount.query() // 100
    savingsAccount.earnMonthlyInterest() // 101
    savingsAccount.withdraw(10)
    savingsAccount.withdraw(10)
    savingsAccount.withdraw(10)
    savingsAccount.query() // 101 - 30 = 71
    savingsAccount.withdraw(10) //
    savingsAccount.query() // 71 - 10 - 1 = 60
  }
}


class BankAccount(initialBalance: Double) {
  private var balance = initialBalance

  def deposit(amount: Double) = {
    balance += amount;
    balance
  }

  def withdraw(amount: Double) = {
    balance -= amount;
    balance
  }

  //加入一个查询余额方法
  def query(): Unit = {
    println("当前余额为" + this.balance)
  }
}

class CheckingAccount(initialBanlance: Double) extends BankAccount(initialBanlance) {
  override def deposit(amount: Double) = super.deposit(amount - 1)

  override def withdraw(amount: Double) = super.withdraw(amount + 1)
}


//这是另外一类银行账号

class SavingsAccount(initialBalance: Double) extends BankAccount(initialBalance) {
  private var num: Int = _ // 定义了一个免手续费次数

  def earnMonthlyInterest() = { // 每个月初，我们系统调用该方法，计算利息，将num=3
    num = 3 //
    super.deposit(1)
  }

  override def deposit(amount: Double): Double = {
    //取款时，如num<0收手续费,否则不收
    num -= 1 //
    if (num < 0)
      super.deposit(amount - 1)
    else
      super.deposit(amount)
  }

  override def withdraw(amount: Double): Double = { //取款逻辑和存款一样
    num -= 1
    if (num < 0) super.withdraw(amount + 1) else super.withdraw(amount)
  }
}
package chapter15.customercrm.service

import chapter15.customercrm.bean.Customer

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class CustomerService {

  //customers是存放客户的，这里我们先初始化,为了测试
  val customers = ArrayBuffer(new Customer(1, "tom", '男', 10, "110", "tom@sohu.com"))
  var customerNum = 1

  def list(): ArrayBuffer[Customer] = {
    this.customers
  }

  //添加客户
  def add(customer: Customer): Boolean = {
    //设置id
    customerNum += 1
    customer.id = customerNum
    //加入到customers
    customers.append(customer)
    true
  }

  def del(id: Int): Boolean = {
    val index = findIndexById(id)
    if (index != -1) {
      //删除
      customers.remove(index)
      true
    } else {
      false
    }
  }

  //根据id找到index
  def findIndexById(id: Int) = {
    var index = -1 //默认-1,如果找到就改成对应,如果没有找到，就返回-1
    //遍历customers
    breakable {
      for (i <- 0 until customers.length) {
        if (customers(i).id == id) { //找到
          index = i
          break()
        }
      }
    }
    index
  }
}

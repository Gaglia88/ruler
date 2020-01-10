package RulER.DataStructure
import scala.collection.mutable

/**
  * Represents a profile
  * @author Giovanni Simonini
  * @since 2016/07/12
  */
case class Profile(id: Long, attributes : mutable.MutableList[KeyValue] = new mutable.MutableList[KeyValue](), originalID : String = "") extends Serializable{

  /**
    * Add an attribute to the list of attributes
    *
    * @param a attribute to add
    * */
  def addAttribute(a: KeyValue): Unit = {
    attributes += a
  }
  // todo If we have no attributes (e.g. a single doc), we have a single element in the list
}

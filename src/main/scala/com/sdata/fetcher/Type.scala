package com.sdata.fetcher



import scala.reflect.ClassTag

/**
 * Created by dejun on 31/1/16.
 */
trait Type {
  /** Apply a transformation to all type children and reconstruct this
    * type with the new children, or return the original object if no
    * child is changed. */
  def mapChildren(f: Type => Type): Type
  /** Apply a side-effecting function to all children. */
  /** The structural view of this type */
  def structural: Type = this
  /** Remove all NominalTypes recursively from this Type */
  def structuralRec: Type = structural.mapChildren(_.structuralRec)
  /** A ClassTag for the erased type of this type's Scala values */
  def classTag: ClassTag[_]
}

object Type {
  /** An extractor for strucural expansions of types */
  object Structural {
    def unapply(t: Type): Some[Type] = Some(t.structural)
  }

}


//trait OptionType extends Type {
//  override def toString = "Option[" + elementType + "]"
//  def elementType: Type
//  def children: ConstArray[Type] = ConstArray(elementType)
//  def classTag = OptionType.classTag
//  override def hashCode = elementType.hashCode() + 100
//  override def equals(o: Any) = o match {
//    case OptionType(elem) if elementType == elem => true
//    case _ => false
//  }
//  override final def childrenForeach[R](f: Type => R): Unit = f(elementType)
//}
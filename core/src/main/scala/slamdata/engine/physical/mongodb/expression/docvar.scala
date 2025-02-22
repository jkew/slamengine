/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.physical.mongodb.expression

import slamdata.Predef._

import scalaz._
import Scalaz._

import slamdata.engine.{Error, RenderTree, Terminal, NonTerminal}
import slamdata.engine.analysis.fixplate.{Term}
import slamdata.engine.javascript._
import slamdata.engine.physical.mongodb.{Bson, BsonField}

object DocField {
  def apply(field: BsonField): DocVar = DocVar.ROOT(field)

  def unapply(docVar: DocVar): Option[BsonField] = docVar match {
    case DocVar.ROOT(tail) => tail
    case _ => None
  }
}
final case class DocVar(name: DocVar.Name, deref: Option[BsonField]) {
  def path: List[BsonField.Leaf] = deref.toList.flatMap(_.flatten.toList)

  def startsWith(that: DocVar) = (this.name == that.name) && {
    (this.deref |@| that.deref)(_ startsWith (_)) getOrElse (that.deref.isEmpty)
  }

  def \ (that: DocVar): Option[DocVar] = (this, that) match {
    case (DocVar(n1, f1), DocVar(n2, f2)) if (n1 == n2) =>
      val f3 = (f1 |@| f2)(_ \ _) orElse (f1) orElse (f2)

      Some(DocVar(n1, f3))

    case _ => None
  }

  def \\ (that: DocVar): DocVar = (this, that) match {
    case (DocVar(n1, f1), DocVar(_, f2)) =>
      val f3 = (f1 |@| f2)(_ \ _) orElse (f1) orElse (f2)

      DocVar(n1, f3)
  }

  def \ (field: BsonField): DocVar = copy(deref = Some(deref.map(_ \ field).getOrElse(field)))

  def toJs: JsFn = JsFn(JsFn.base, this match {
    case DocVar(_, None)        => JsFn.base.fix
    case DocVar(_, Some(deref)) => deref.toJs(JsFn.base.fix)
  })

  override def toString = this match {
    case DocVar(DocVar.ROOT, None) => "DocVar.ROOT()"
    case DocVar(DocVar.ROOT, Some(deref)) => s"DocField($deref)"
    case _ => s"DocVar($name, $deref)"
  }

  def bson: Bson = this match {
    case DocVar(DocVar.ROOT, Some(deref)) => Bson.Text(deref.asField)
    case DocVar(name,        deref) =>
      val root = BsonField.Name(name.name)
        Bson.Text(deref.map(root \ _).getOrElse(root).asVar)
  }
}
object DocVar {
  final case class Name(name: String) {
    def apply() = DocVar(this, None)

    def apply(field: BsonField) = DocVar(this, Some(field))

    def apply(deref: Option[BsonField]) = DocVar(this, deref)

    def apply(leaves: List[BsonField.Leaf]) = DocVar(this, BsonField(leaves))

    def unapply(v: DocVar): Option[Option[BsonField]] = Some(v.deref)
  }
  val ROOT    = Name("ROOT")
  val CURRENT = Name("CURRENT")
}

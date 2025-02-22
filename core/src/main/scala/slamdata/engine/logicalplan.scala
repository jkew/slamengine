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

package slamdata.engine

import slamdata.Predef._

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.fs.Path

import slamdata.engine.analysis._
import fixplate._

sealed trait LogicalPlan[A]
object LogicalPlan {
  import slamdata.engine.std.StdLib._
  import identity._
  import set._
  import structural._

  implicit val LogicalPlanTraverse = new Traverse[LogicalPlan] {
    def traverseImpl[G[_], A, B](fa: LogicalPlan[A])(f: A => G[B])(implicit G: Applicative[G]): G[LogicalPlan[B]] = {
      fa match {
        case ReadF(coll) => G.point(ReadF(coll))
        case ConstantF(data) => G.point(ConstantF(data))
        case JoinF(left, right, tpe, rel) =>
          G.apply3(f(left), f(right), f(rel))(JoinF(_, _, tpe, _))
        case InvokeF(func, values) => G.map(Traverse[List].sequence(values.map(f)))(InvokeF(func, _))
        case FreeF(v) => G.point(FreeF(v))
        case LetF(ident, form0, in0) =>
          G.apply2(f(form0), f(in0))(LetF(ident, _, _))
      }
    }

    override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] = {
      v match {
        case ReadF(coll) => ReadF(coll)
        case ConstantF(data) => ConstantF(data)
        case JoinF(left, right, tpe, rel) =>
          JoinF(f(left), f(right), tpe, f(rel))
        case InvokeF(func, values) => InvokeF(func, values.map(f))
        case FreeF(v) => FreeF(v)
        case LetF(ident, form, in) => LetF(ident, f(form), f(in))
      }
    }

    override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit F: Monoid[B]): B = {
      fa match {
        case ReadF(_) => F.zero
        case ConstantF(_) => F.zero
        case JoinF(left, right, tpe, rel) =>
          F.append(F.append(f(left), f(right)), f(rel))
        case InvokeF(func, values) => Foldable[List].foldMap(values)(f)
        case FreeF(_) => F.zero
        case LetF(_, form, in) => {
          F.append(f(form), f(in))
        }
      }
    }

    override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B = {
      fa match {
        case ReadF(_) => z
        case ConstantF(_) => z
        case JoinF(left, right, tpe, rel) => f(left, f(right, f(rel, z)))
        case InvokeF(func, values) => Foldable[List].foldRight(values, z)(f)
        case FreeF(_) => z
        case LetF(ident, form, in) => f(form, f(in, z))
      }
    }
  }
  implicit val RenderTreeLogicalPlan: RenderTree[LogicalPlan[_]] = new RenderTree[LogicalPlan[_]] {
    val nodeType = "LogicalPlan" :: Nil

    // Note: these are all terminals; the wrapping Term or Cofree will use these to build nodes with children.
    override def render(v: LogicalPlan[_]) = v match {
      case ReadF(name)                 => Terminal("Read" :: nodeType, Some(name.pathname))
      case ConstantF(data)             => Terminal("Constant" :: nodeType, Some(data.toString))
      case JoinF(_, _, tpe, _) => Terminal("Join" :: nodeType, Some(tpe.toString))
      case InvokeF(func, _     )       => Terminal(func.mappingType.toString :: "Invoke" :: nodeType, Some(func.name))
      case FreeF(name)                 => Terminal("Free" :: nodeType, Some(name.toString))
      case LetF(ident, _, _)           => Terminal("Let" :: nodeType, Some(ident.toString))
    }
  }
  implicit val EqualFLogicalPlan = new fp.EqualF[LogicalPlan] {
    def equal[A](v1: LogicalPlan[A], v2: LogicalPlan[A])(implicit A: Equal[A]): Boolean = (v1, v2) match {
      case (ReadF(n1), ReadF(n2)) => n1 == n2
      case (ConstantF(d1), ConstantF(d2)) => d1 == d2
      case (JoinF(l1, r1, tpe1, rel1),
            JoinF(l2, r2, tpe2, rel2)) =>
        A.equal(l1, l2) && A.equal(r1, r2) && A.equal(rel1, rel2) && tpe1 == tpe2
      case (InvokeF(f1, v1), InvokeF(f2, v2)) => Equal[List[A]].equal(v1, v2) && f1 == f2
      case (FreeF(n1), FreeF(n2)) => n1 == n2
      case (LetF(ident1, form1, in1), LetF(ident2, form2, in2)) =>
        ident1 == ident2 && A.equal(form1, form2) && A.equal(in1, in2)
      case _ => false
    }
  }

  final case class ReadF[A](path: Path) extends LogicalPlan[A] {
    override def toString = s"""Read(Path("${path.simplePathname}"))"""
  }
  object Read {
    def apply(path: Path): Term[LogicalPlan] =
      Term[LogicalPlan](new ReadF(path))
  }

  final case class ConstantF[A](data: Data) extends LogicalPlan[A]
  object Constant {
    def apply(data: Data): Term[LogicalPlan] =
      Term[LogicalPlan](ConstantF(data))
  }

  final case class JoinF[A](left: A, right: A, joinType: JoinType, joinRel: A)
      extends LogicalPlan[A] {
    override def toString = s"Join($left, $right, $joinType, $joinRel)"
  }
  object Join {
    def apply(left: Term[LogicalPlan], right: Term[LogicalPlan],
              joinType: JoinType, joinRel: Term[LogicalPlan]): Term[LogicalPlan] =
      Term[LogicalPlan](JoinF(left, right, joinType, joinRel))
  }

  final case class InvokeF[A](func: Func, values: List[A]) extends LogicalPlan[A] {
    override def toString = {
      val funcName = if (func.name(0).isLetter) func.name.split('_').map(_.toLowerCase.capitalize).mkString
                      else "\"" + func.name + "\""
      funcName + "(" + values.mkString(", ") + ")"
    }
  }
  object Invoke {
    def apply(func: Func, values: List[Term[LogicalPlan]]): Term[LogicalPlan] =
      Term[LogicalPlan](InvokeF(func, values))
  }

  final case class FreeF[A](name: Symbol) extends LogicalPlan[A]
  object Free {
    def apply(name: Symbol): Term[LogicalPlan] =
      Term[LogicalPlan](FreeF(name))
  }

  final case class LetF[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]
  object Let {
    def apply(let: Symbol, form: Term[LogicalPlan], in: Term[LogicalPlan]): Term[LogicalPlan] =
      Term[LogicalPlan](LetF(let, form, in))
  }

  implicit val LogicalPlanUnzip = new Unzip[LogicalPlan] {
    def unzip[A, B](f: LogicalPlan[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val LogicalPlanBinder = new Binder[LogicalPlan] {
      type G[A] = Map[Symbol, A]

      def initial[A] = Map[Symbol, A]()

      def bindings[A](t: LogicalPlan[Term[LogicalPlan]], b: G[A])(f: LogicalPlan[Term[LogicalPlan]] => A): G[A] =
        t match {
          case LetF(ident, form, _) => b + (ident -> f(form.unFix))
          case _                    => b
        }

      def subst[A](t: LogicalPlan[Term[LogicalPlan]], b: G[A]): Option[A] =
        t match {
          case FreeF(symbol) => b.get(symbol)
          case _             => None
        }
    }

  val namesƒ: LogicalPlan[Set[Symbol]] => Set[Symbol] = {
    case FreeF(name) => Set(name)
    case x           => x.fold
  }

  def freshName[F[_]: Functor: Foldable](
    prefix: String, plans: F[Term[LogicalPlan]]):
      Symbol = {
    val existingNames = plans.map(_.cata(namesƒ)).fold
    def loop(pre: String): Symbol =
      if (existingNames.contains(Symbol(prefix)))
        loop(pre + "_")
      else Symbol(prefix)

    loop(prefix)
  }

  val shapeƒ: LogicalPlan[(Term[LogicalPlan], Option[List[Term[LogicalPlan]]])] => Option[List[Term[LogicalPlan]]] = {
    case JoinF(left, right, _, _) =>
      List(left._2, right._2).sequence.map(_.flatten)
    case LetF(_, _, body) => body._2
    case ConstantF(Data.Obj(map)) =>
      Some(map.keys.map(n => Constant(Data.Str(n))).toList)
    case InvokeF(DeleteField, List(src, field)) =>
      src._2.map(_.filterNot(_ == field._1))
    case InvokeF(MakeObject, List(field, src)) => Some(List(field._1))
    case InvokeF(ObjectConcat, srcs) => srcs.map(_._2).sequence.map(_.flatten)
    // NB: the remaining InvokeF cases simply pass through or combine shapes
    //     from their inputs. It would be great if this information could be
    //     handled generically by the type system.
    case InvokeF(OrderBy, List(src, _, _)) => src._2
    case InvokeF(Take, List(src, _)) => src._2
    case InvokeF(Drop, List(src, _)) => src._2
    case InvokeF(Filter, List(src, _)) => src._2
    case InvokeF(Cross, srcs) => srcs.map(_._2).sequence.map(_.flatten)
    case InvokeF(GroupBy, List(src, _)) => src._2
    case InvokeF(Distinct, List(src, _)) => src._2
    case InvokeF(DistinctBy, List(src, _)) => src._2
    case InvokeF(Squash, List(src)) => src._2
    case _ => None
  }

  // TODO: Generalize this to Binder
  def lpParaZygoHistoM[M[_]: Monad, A, B](
    t: Term[LogicalPlan])(
    f: LogicalPlan[(Term[LogicalPlan], B)] => B,
    g: LogicalPlan[Cofree[LogicalPlan, (B, A)]] => M[A]):
      M[A] = {
    def loop(t: Term[LogicalPlan], bind: Map[Symbol, Cofree[LogicalPlan, (B, A)]]):
        M[Cofree[LogicalPlan, (B, A)]] = {
      lazy val default: M[Cofree[LogicalPlan, (B, A)]] = for {
        lp <- (t.unFix.map(x => for {
          co <- loop(x, bind)
        } yield ((x, co.head._1), co))).sequence
        (xb, co) = lp.unfzip
        b = f(xb)
        a <- g(co)
      } yield Cofree((b, a), co)

      t.unFix match {
        case FreeF(name)            => bind.get(name).fold(default)(_.point[M])
        case LetF(name, form, body) => for {
          form1 <- loop(form, bind)
          rez   <- loop(body, bind + (name -> form1))
        } yield rez
        case _                      => default
      }
    }

    for {
      rez <- loop(t, Map())
    } yield rez.head._2
  }

  def lpParaZygoHistoS[S, A, B] = lpParaZygoHistoM[State[S, ?], A, B] _
  def lpParaZygoHisto[A, B] = lpParaZygoHistoM[Id, A, B] _

  sealed trait JoinType
  object JoinType {
    final case object Inner extends JoinType
    final case object LeftOuter extends JoinType
    final case object RightOuter extends JoinType
    final case object FullOuter extends JoinType
  }
}

package sdp.result

import cats._
import cats.data.EitherT
import cats.data.OptionT
import monix.eval.Task
import cats.implicits._

import scala.concurrent._

import scala.collection.TraversableOnce

object DBOResult {


  type DBOError[A] = EitherT[Task,Throwable,A]
  type DBOResult[A] = OptionT[DBOError,A]

  implicit def valueToDBOResult[A](a: A): DBOResult[A] =
         Applicative[DBOResult].pure(a)
  implicit def optionToDBOResult[A](o: Option[A]): DBOResult[A] =
         OptionT((o: Option[A]).pure[DBOError])
  implicit def eitherToDBOResult[A](e: Either[Throwable,A]): DBOResult[A] = {
 //   val error: DBOError[A] = EitherT[Task,Throwable, A](Task.eval(e))
         OptionT.liftF(EitherT.fromEither[Task](e))
  }
  implicit def futureToDBOResult[A](fut: Future[A]): DBOResult[A] = {
       val task = Task.fromFuture[A](fut)
       val et = EitherT.liftF[Task,Throwable,A](task)
       OptionT.liftF(et)
  }

  implicit class DBOResultToTask[A](r: DBOResult[A]) {
    def toTask = r.value.value
  }

  implicit class DBOResultToOption[A](r:Either[Throwable,Option[A]]) {
    def someValue: Option[A] = r match {
      case Left(err) => (None: Option[A])
      case Right(oa) => oa
    }
  }

  def wrapCollectionInOption[A, C[_] <: TraversableOnce[_]](coll: C[A]): DBOResult[C[A]] =
    if (coll.isEmpty)
      optionToDBOResult(None: Option[C[A]])
    else
      optionToDBOResult(Some(coll): Option[C[A]])
}

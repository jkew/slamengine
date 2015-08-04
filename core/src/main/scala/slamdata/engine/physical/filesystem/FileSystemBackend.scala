package slamdata.engine.physical.filesystem

import slamdata.Predef
import slamdata.Predef.{String, Set, Unit}
import slamdata.engine.Backend._
import slamdata.engine.Planner.EitherWriter
import slamdata.engine._
import slamdata.engine.analysis.fixplate
import slamdata.engine.config.{FileSystemBackendConfig, BackendConfig}
import slamdata.engine.fs.{WriteError, Path}
import slamdata.engine.fp._

import java.lang.Process

/**
 * Created by jkew on 8/3/15.
 */

sealed trait WorkflowFS
object Workflow {

}

class FileSystemBackend(fsConfig:FileSystemBackendConfig) extends PlannerBackend[WorkflowFS] {
  override def planner: Planner[WorkflowFS] = new FileSystemPlanner()

  override def evaluator: Evaluator[WorkflowFS] = new FileSystemEvaluator()

  override implicit def RP: RenderTree[WorkflowFS] = ???

  override def ls0(dir: Path): PathTask[Set[FilesystemNode]] = liftP(Set(FilesystemNode(Path(fsConfig.localPath), Mount)))

  override def count0(path: Path): PathTask[Predef.Long] = liftP(1L)

  override def move0(src: Path, dst: Path, semantics: MoveSemantics): PathTask[Unit] = liftP({})

  override def defaultPath: Path = Path(fsConfig.localPath)

  override def delete0(path: Path): PathTask[Unit] = liftP({})

  override def append0(path: Path, values: Process[Any, Data]): Process[PathTask, WriteError] = new Process[PathTask, WriteError]() {}

  override def scan0(path: Path, offset: Predef.Option[Predef.Long], limit: Predef.Option[Predef.Long]): Process[PathTask, Data] = ???

  override def save0(path: Path, values: Process[Any, Data]): PathTask[Unit] = ???
}



class FileSystemPlanner extends Planner[WorkflowFS] {
  override def plan(logical: fixplate.Term[LogicalPlan]): EitherWriter[WorkflowFS] = ???
}

class FileSystemEvaluator extends Evaluator[WorkflowFS] {
  /**
   * Executes the specified physical plan.
   *
   * Returns the location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified
   * destination resource (because this would require copying all the data).
   */
  override def execute(physical: WorkflowFS) = ???

  /**
   * Fails if the backend implementation is not compatible with the connected
   * system (typically because it does not have not the correct version number).
   */
  override def checkCompatibility = ???

  /**
   * Compile the specified physical plan to a command
   * that can be run natively on the backend.
   */
  override def compile(physical: WorkflowFS): (String, Any) = ???
}
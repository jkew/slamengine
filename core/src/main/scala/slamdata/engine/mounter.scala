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

import slamdata.engine.config._
import slamdata.engine.fs._

import scalaz._
import Scalaz._

import scalaz.concurrent._

object Mounter {
  sealed trait MountError extends Error
  final case class MissingFileSystem(path: Path, config: BackendConfig) extends MountError {
    def message = "No data source could be mounted at the path " + path + " using the config " + config
  }
  final case class InvalidConfig(message: String) extends MountError

  def mountE(config: Config): EitherT[Task, MountError, Backend] = {
    def rec(backend: Backend, path: List[DirNode], conf: BackendConfig): EitherT[Task, MountError, Backend] =
      backend match {
        case NestedBackend(base) =>
          path match {
            case Nil => BackendDefinitions.All(conf).fold[EitherT[Task, MountError, Backend]](
              EitherT.left(Task.now(MissingFileSystem(Path(path, None), conf))))(
              EitherT.right)
            case dir :: dirs =>
              rec(base.get(dir).getOrElse(NestedBackend(Map())), dirs, conf).map(rez => NestedBackend(base + (dir -> rez)))
          }
        case _ => EitherT.left(Task.now(InvalidConfig("attempting to mount a backend within an existing backend.")))
      }

    config.mountings.foldLeft[EitherT[Task, MountError, Backend]](
      EitherT.right(Task.now(NestedBackend(Map())))) {
      case (root, (path, config)) =>
        root.flatMap(rec(_, path.asAbsolute.asDir.dir, config))
    }
  }

  def mount(config: Config): Task[Backend] =
    mountE(config).fold(Task.fail, Task.now).join
}

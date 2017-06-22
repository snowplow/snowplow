/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader

import cats.data.{EitherT, Validated, ValidatedNel}
import cats.implicits._
import cats.free.Free
import ShreddedType._
import LoaderError.{AtomicDiscoveryFailure, DiscoveryError, DiscoveryFailure, NoDataDiscovered}
import config.Semver

/**
 * Result of data discovery in shredded.good folder
 */
sealed trait DataDiscovery extends Product with Serializable {
  /**
   * Shred run folder full path
   */
  def base: S3.Folder = S3.Folder.getParent(atomicEvents)

  /**
   * `atomic-events` directory full path
   */
  def atomicEvents = S3.Key.getParent(atomicData.head)

  /**
   * List of files in `atomic-events` dir
   * Exactly one `atomic-events` directory must be present
   */
  def atomicData: DataDiscovery.AtomicData
}

/**
 * This module provides data-discovery mechanism for "atomic" events only
 * and for "full" discovery (atomic and shredded types)
 * Primary methods return lists of `DataDiscovery` results, where
 * each `DataDiscovery` represents particular `run=*` folder in shredded.good
 *
 * It lists all keys in run id folder, parse each one as atomic-events key or
 * shredded type key and then groups by run ids
 */
object DataDiscovery {

  /**
   * All objects from single `atomic-events` directory
   */
  type AtomicData = List[S3.Key]

  /**
   * Shredded types, each containing whole list of objects in it
   */
  type ShreddedData = Map[ShreddedType, List[S3.Key]]

  /**
   * Discovery result that contains only atomic data (list of S3 keys in `atomic-events`)
   */
  case class AtomicDiscovery(atomicData: AtomicData) extends DataDiscovery

  /**
   * Full discovery result that contains both atomic data (list of S3 keys in `atomic-events`)
   * and shredded data
   */
  case class FullDiscovery(atomicData: AtomicData, shreddedTypes: ShreddedData) extends DataDiscovery

  /**
   * Discover list of shred run folders, each containing
   * exactly one `atomic-events` folder and zero or more `ShreddedType`s
   *
   * Action fails if:
   * + no atomic-events folder found in *any* shred run folder
   * + files with unknown path were found in *any* shred run folder
   *
   * @param shreddedGood shredded good S3 folder
   * @param shredJob shred job version to check path pattern
   * @param region AWS region for S3 buckets
   * @param assets optional JSONPath assets S3 bucket
   * @return non-empty list (usually with single element) of discover results
   *         (atomic events and shredded types)
   */
  def discoverFull(shreddedGood: S3.Folder, shredJob: Semver, region: String, assets: Option[S3.Folder]): Discovery[List[DataDiscovery]] = {
    val validatedDataKeys: Discovery[ValidatedDataKeys] =
      Discovery.map(listGoodBucket(shreddedGood))(transformKeys(shredJob, region, assets))

    val result = for {
      keys <- EitherT(validatedDataKeys)
      discovery <- EitherT(groupKeysFull(keys))
      list <- EitherT.fromEither[Action](checkNonEmpty(discovery))
    } yield list
    result.value
  }

  /**
   * Discovery list of `atomic-events` folders
   *
   * @param shreddedGood shredded good S3 folder
   * @return list of run folders with `atomic-events`
   */
  def discoverAtomic(shreddedGood: S3.Folder): Discovery[List[DataDiscovery]] =
    Discovery.map(listGoodBucket(shreddedGood))(groupKeysAtomic).map(_.flatten)

  /**
   * List whole directory excluding special files
   */
  def listGoodBucket(shreddedGood: S3.Folder): Discovery[List[S3.Key]] =
    Discovery.map(LoaderA.listS3(shreddedGood))(_.filterNot(isSpecial))

  // Full discovery

  /**
   * Group list of keys into list (usually single-element) of `FullDiscovery`
   *
   * @param validatedDataKeys IO-action producing validated list of `FinalDataKey`
   * @return IO-action producing list of
   */
  def groupKeysFull(validatedDataKeys: ValidatedDataKeys): Discovery[List[DataDiscovery]] = {
    def group(dataKeys: List[DataKeyFinal]): ValidatedNel[DiscoveryFailure, List[DataDiscovery]] =
      dataKeys.groupBy(_.base).toList.reverse.traverse(validateFolderFull)

    // Transform into Either with non-empty list of errors
    validatedDataKeys.map { keys =>
      keys.andThen(group) match {
        case Validated.Valid(discovery) => Right(discovery)
        case Validated.Invalid(failures) =>
          val aggregated = LoaderError.aggregateDiscoveryFailures(failures.toList)
          Left(DiscoveryError(aggregated))
      }
    }
  }

  /**
   * Check that S3 folder contains at least one atomic-events file
   * And split `FinalDataKey`s into "atomic" and "shredded"
   */
  def validateFolderFull(groupOfKeys: (S3.Folder, List[DataKeyFinal])): ValidatedNel[DiscoveryFailure, DataDiscovery] = {
    val empty = (List.empty[AtomicDataKey], List.empty[ShreddedDataKeyFinal])
    val (base, dataKeys) = groupOfKeys
    val (atomicKeys, shreddedKeys) = dataKeys.foldLeft(empty) { case ((atomicKeys, shreddedTypes), key) =>
      key match {
        case atomicKey: AtomicDataKey => (atomicKey :: atomicKeys, shreddedTypes)
        case shreddedType: ShreddedDataKeyFinal => (atomicKeys, shreddedType :: shreddedTypes)
      }
    }

    if (atomicKeys.nonEmpty) {
      val shreddedData = shreddedKeys.groupBy(_.info).mapValues(dataKeys => dataKeys.map(_.key).reverse)
      FullDiscovery(atomicKeys.map(_.key).reverse, shreddedData).validNel
    } else {
      AtomicDiscoveryFailure(base).invalidNel
    }
  }

  /**
   * Transform list of S3 keys into list of `DataKeyFinal` for `FullDisocvery`
   */
  private def transformKeys(shredJob: Semver, region: String, assets: Option[S3.Folder])(keys: List[S3.Key]): ValidatedDataKeys = {
    val intermediateDataKeys = keys.map(parseDataKey(shredJob, _))
    intermediateDataKeys.map(transformDataKey(_, region, assets)).sequence.map(_.sequence)
  }
  
  /**
   * Transform intermediate `DataKey` into `ReadyDataKey` by finding JSONPath file
   * for each shredded type. Used to aggregate "invalid path" errors (produced by
   * `parseDataKey`) with "not found JSONPath"
   * Atomic data will be returned as-is
   *
   * @param dataKey either successful or failed data key
   * @param region AWS region for S3 buckets
   * @param assets optional JSONPath assets S3 bucket
   * @return `Action` conaining `Validation` - as on next step we can aggregate errors
   */
  private def transformDataKey(dataKey: DiscoveryStep[DataKeyIntermediate], region: String, assets: Option[S3.Folder]) = {
    dataKey match {
      case Right(ShreddedDataKeyIntermediate(fullPath, info)) =>
        val jsonpathAction = ShreddedType.discoverJsonPath(region, assets, info)
        val discoveryAction: DiscoveryAction[DataKeyFinal] =
          DiscoveryAction.map(jsonpathAction) { jsonpath =>
            ShreddedDataKeyFinal(fullPath, ShreddedType(info, jsonpath))
          }
        discoveryAction.map(_.toValidatedNel)
      case Right(AtomicDataKey(fullPath)) =>
        val pure: Action[ValidatedNel[DiscoveryFailure, DataKeyFinal]] =
          Free.pure(AtomicDataKey(fullPath).validNel[DiscoveryFailure])
        pure
      case Left(failure) =>
        val pure: Action[ValidatedNel[DiscoveryFailure, DataKeyFinal]] =
          Free.pure(failure.invalidNel)
        pure
    }
  }

  /**
   * Parse S3 key into valid atomic-events path or shredded type
   * This function will short-circuit whole discover process if found
   * any invalid S3 key - not atomic events file neither shredded type file
   *
   * @param shredJob shred job version to check path pattern
   * @param key particular S3 key in shredded good folder
   * @return
   */
  private def parseDataKey(shredJob: Semver, key: S3.Key): Either[DiscoveryFailure, DataKeyIntermediate] = {
    S3.getAtomicPath(key) match {
      case Some(_) => AtomicDataKey(key).asRight
      case None =>
        ShreddedType.transformPath(key, shredJob) match {
          case Right(info) =>
            ShreddedDataKeyIntermediate(key, info).asRight
          case Left(e) => e.asLeft
        }
    }
  }

  // Atomic discovery

  /**
   * Group list of S3 keys by run folder
   */
  def groupKeysAtomic(keys: List[S3.Key]): Either[DiscoveryError, List[AtomicDiscovery]] = {
    val atomicKeys: ValidatedNel[DiscoveryFailure, List[AtomicDataKey]] =
      keys.filter(isAtomic).map(parseAtomicKey).map(_.toValidatedNel).sequence

    val validated = atomicKeys.andThen { keys => keys.groupBy(_.base).toList.traverse(validateFolderAtomic) }

    validated match {
      case Validated.Valid(x) => x.asRight
      case Validated.Invalid(x) => DiscoveryError(x.toList).asLeft
    }
  }

  /**
   * Check that `atomic-events` folder is non-empty
   */
  def validateFolderAtomic(groupOfKeys: (S3.Folder, List[AtomicDataKey])): ValidatedNel[DiscoveryFailure, AtomicDiscovery] = {
    val (base, keys) = groupOfKeys
    if (keys.nonEmpty) {
      AtomicDiscovery(keys.map(_.key)).validNel
    } else {
      AtomicDiscoveryFailure(base).invalidNel
    }
  }

  /**
   * Check if S3 key is proper object from `atomic-events`
   */
  private def parseAtomicKey(key: S3.Key): Either[DiscoveryFailure, AtomicDataKey] = {
    S3.getAtomicPath(key) match {
      case Some(_) => AtomicDataKey(key).asRight
      case None => AtomicDiscoveryFailure(key).asLeft
    }
  }

  // Common

  def isAtomic(key: S3.Key): Boolean = key match {
    case loaders.Common.atomicSubpathPattern(_, _, _) => true
    case _ => false
  }

  def isSpecial(key: S3.Key): Boolean = key.contains("$") || key == "_SUCCESS"

  // Consistency

  /**
   * Wait until S3 list result becomes consistent
   * Waits some time after initial request, depending on cardinality of discovered folders
   * then does identical request, and compares results
   * If results differ - wait and request again. Repeat 5 times until requests identical
   *
   * @param originalAction data-discovery action
   * @return result of same request, but with more guarantees to be consistent
   */
  def checkConsistency(originalAction: Discovery[List[DataDiscovery]]): Discovery[List[DataDiscovery]] = {
    def check(checkAttempt: Int, last: Option[Either[DiscoveryError, List[DataDiscovery]]]): Discovery[List[DataDiscovery]] = {
      val action = last.map(Free.pure[LoaderA, Either[DiscoveryError, List[DataDiscovery]]]).getOrElse(originalAction)

      for {
        original <- action
        _        <- sleepConsistency(original)
        control  <- originalAction
        result   <- retry(original, control, checkAttempt + 1)
      } yield result
    }

    def retry(
      original: Either[DiscoveryError, List[DataDiscovery]],
      control: Either[DiscoveryError, List[DataDiscovery]],
      attempt: Int): Discovery[List[DataDiscovery]] = {
      if (attempt >= 5)
        Free.pure(control.orElse(original))
      else if (original.isRight && original == control)
        Free.pure(original)
      else if (control.isLeft || original.isLeft)
        check(attempt, None)
      else // Both Right, but not equal
        check(attempt, Some(control))
    }

    check(1, None)
  }

  /**
   * Check that list of discovered folders is non-empty
   */
  private def checkNonEmpty(discovery: List[DataDiscovery]): Either[DiscoveryError, List[DataDiscovery]] =
    if (discovery.isEmpty)
      Left(DiscoveryError(List(NoDataDiscovered)))
    else
      Right(discovery)

  /**
   * Aggregates wait time for all discovered folders or wait 10 sec in case action failed
   */
  private def sleepConsistency(result: Either[DiscoveryError, List[DataDiscovery]]): Action[Unit] = {
    val timeoutMs = result match {
      case Right(list) =>
        list.map(_.consistencyTimeout).foldLeft(10000L)(_ + _)
      case Left(_) => 10000L
    }

    LoaderA.sleep(timeoutMs)
  }


  // Temporary "type tags", representing validated "atomic" or "shredded" S3 keys

  /**
   * S3 key, representing either atomic events file or shredded type file
   * It is intermediate because shredded type doesn't contain its JSONPath yet
   */
  private sealed trait DataKeyIntermediate

  /**
   * S3 key, representing either atomic events file or shredded type file
   * It is final because shredded type proved to have JSONPath
   */
  private sealed trait DataKeyFinal {
    def base: S3.Folder
    def key: S3.Key
  }

  /**
   * S3 key, representing atomic events file
   * It can be used as both "intermediate" and "final" because atomic-events
   * don't need any more validations, except path
   */
  private case class AtomicDataKey(key: S3.Key) extends DataKeyIntermediate with DataKeyFinal {
    def base: S3.Folder = {
      val atomicEvents = S3.Key.getParent(key)
      S3.Folder.getParent(atomicEvents)
    }
  }

  /**
   * S3 key, representing intermediate shredded type file
   * It is intermediate because shredded type doesn't contain its JSONPath yet
   */
  private case class ShreddedDataKeyIntermediate(key: S3.Key, info: Info) extends DataKeyIntermediate

  /**
   * S3 key, representing intermediate shredded type file
   * It is final because shredded type proved to have JSONPath
   */
  private case class ShreddedDataKeyFinal(key: S3.Key, info: ShreddedType) extends DataKeyFinal {
    def base: S3.Folder = info.info.base
  }

  private type ValidatedDataKeys = Action[ValidatedNel[DiscoveryFailure, List[DataKeyFinal]]]
}

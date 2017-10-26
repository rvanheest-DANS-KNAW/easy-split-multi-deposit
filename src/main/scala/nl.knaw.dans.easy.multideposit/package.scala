/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy

import java.io.IOException
import java.nio.charset.{ Charset, StandardCharsets }

import better.files._
import nl.knaw.dans.easy.multideposit.model.DepositId
import org.apache.commons.lang.StringUtils

import scala.util.Try
import scala.xml.{ Elem, XML }

package object multideposit {

  type Datamanager = String
  type DatamanagerEmailaddress = String

  case class DepositPermissions(permissions: String, group: String)
  case class Settings(multidepositDir: File = null,
                      stagingDir: File = null,
                      outputDepositDir: File = null,
                      datamanager: Datamanager = null,
                      depositPermissions: DepositPermissions = null,
                      formats: Set[String] = Set.empty[String],
                      ldap: Ldap = null) {
    override def toString: String =
      s"Settings(multideposit-dir=$multidepositDir, " +
        s"staging-dir=$stagingDir, " +
        s"output-deposit-dir=$outputDepositDir" +
        s"datamanager=$datamanager, " +
        s"deposit-permissions=$depositPermissions, " +
        s"formats=${ formats.mkString("{", ", ", "}") })"
  }

  case class EmptyInstructionsFileException(file: File) extends Exception(s"The given instructions file in '$file' is empty")
  case class ParserFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class PreconditionsFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class ActionRunFailedException(report: String, cause: Throwable = null) extends Exception(report, cause)
  case class ParseException(row: Int, message: String, cause: Throwable = null) extends Exception(message, cause)
  case class ActionException(row: Int, message: String, cause: Throwable = null) extends Exception(message, cause)

  implicit class StringExtensions(val s: String) extends AnyVal {
    /**
     * Checks whether the `String` is blank
     * (according to org.apache.commons.lang.StringUtils.isBlank)
     *
     * @return
     */
    def isBlank: Boolean = StringUtils.isBlank(s)

    /**
     * Converts a `String` to an `Option[String]`. If the `String` is blank
     * (according to org.apache.commons.lang.StringUtils.isBlank)
     * the empty `Option` is returned, otherwise the `String` is returned
     * wrapped in an `Option`.
     *
     * @return an `Option` of the input string that indicates whether it is blank
     */
    def toOption: Option[String] = {
      if (s.isBlank) Option.empty
      else Option(s)
    }

    /**
     * Converts a `String` into an `Option[Int]` if it is not blank
     * (according to org.apache.commons.lang.StringUtils.isBlank).
     * Strings that do not represent a number will yield an empty `Option`.
     *
     * @return an `Option` of the input string, converted as a number if it is not blank
     */
    def toIntOption: Option[Int] = {
      Try {
        if (s.isBlank) Option.empty
        else Option(s.toInt)
      } getOrElse Option.empty
    }
  }

  implicit class FileExtensions(val file: File) extends AnyVal {
    /**
     * Writes the xml to `file` and prepends a simple xml header: `<?xml version="1.0" encoding="UTF-8"?>`
     *
     * @param elem     the xml to be written
     * @param encoding the encoding applied to this xml
     */
    @throws[IOException]("in case of an I/O error")
    def writeXml(elem: Elem, encoding: Charset = encoding): Unit = {
      file.parent.createDirectories()
      XML.save(file.toString, elem, encoding.toString, xmlDecl = true)
    }
  }

  val encoding = StandardCharsets.UTF_8
  val bagDirName = "bag"
  val dataDirName = "data"
  val metadataDirName = "metadata"
  val instructionsFileName = "instructions.csv"
  val datasetMetadataFileName = "dataset.xml"
  val fileMetadataFileName = "files.xml"
  val propsFileName = "deposit.properties"

  private def datasetDir(depositId: DepositId)(implicit settings: Settings): String = {
    s"${ settings.multidepositDir.name }-$depositId"
  }

  def multiDepositInstructionsFile(baseDir: File): File = {
    baseDir / instructionsFileName
  }

  // mdDir/depositId/
  def multiDepositDir(depositId: DepositId)(implicit settings: Settings): File = {
    settings.multidepositDir / depositId
  }

  // mdDir/instructions.csv
  def multiDepositInstructionsFile(implicit settings: Settings): File = {
    multiDepositInstructionsFile(settings.multidepositDir)
  }

  // stagingDir/mdDir-depositId/
  def stagingDir(depositId: DepositId)(implicit settings: Settings): File = {
    settings.stagingDir / datasetDir(depositId)
  }

  // stagingDir/mdDir-depositId/bag/
  def stagingBagDir(depositId: DepositId)(implicit settings: Settings): File = {
    stagingDir(depositId) / bagDirName
  }

  // stagingDir/mdDir-depositId/bag/data/
  def stagingBagDataDir(depositId: DepositId)(implicit settings: Settings): File = {
    stagingBagDir(depositId) / dataDirName
  }

  // stagingDir/mdDir-depositId/bag/metadata/
  def stagingBagMetadataDir(depositId: DepositId)(implicit settings: Settings): File = {
    stagingBagDir(depositId) / metadataDirName
  }

  // stagingDir/mdDir-depositId/deposit.properties
  def stagingPropertiesFile(depositId: DepositId)(implicit settings: Settings): File = {
    stagingDir(depositId) / propsFileName
  }

  // stagingDir/mdDir-depositId/bag/metadata/dataset.xml
  def stagingDatasetMetadataFile(depositId: DepositId)(implicit settings: Settings): File = {
    stagingBagMetadataDir(depositId) / datasetMetadataFileName
  }

  // stagingDir/mdDir-depositId/bag/metadata/files.xml
  def stagingFileMetadataFile(depositId: DepositId)(implicit settings: Settings): File = {
    stagingBagMetadataDir(depositId) / fileMetadataFileName
  }

  // outputDepositDir/mdDir-depositId/
  def outputDepositDir(depositId: DepositId)(implicit settings: Settings): File = {
    settings.outputDepositDir / datasetDir(depositId)
  }
}

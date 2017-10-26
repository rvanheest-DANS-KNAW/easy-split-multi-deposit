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
package nl.knaw.dans.easy.multideposit.actions

import better.files._
import nl.knaw.dans.easy.multideposit.actions.AddFileMetadataToDeposit._
import nl.knaw.dans.easy.multideposit.model.{ Deposit, FileAccessRights, Subtitles }
import nl.knaw.dans.easy.multideposit.{ Settings, UnitAction, _ }
import nl.knaw.dans.lib.error._
import org.apache.tika.Tika

import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }
import scala.xml.{ Elem, NodeSeq }

case class AddFileMetadataToDeposit(deposit: Deposit)(implicit settings: Settings) extends UnitAction[Unit] {

  sealed abstract class AudioVideo(val vocabulary: String)
  case object Audio extends AudioVideo("http://schema.org/AudioObject")
  case object Video extends AudioVideo("http://schema.org/VideoObject")

  sealed abstract class FileMetadata(val filepath: File,
                                     val mimeType: MimeType)
  case class DefaultFileMetadata(override val filepath: File,
                                 override val mimeType: MimeType,
                                 title: Option[String] = Option.empty,
                                 accessibleTo: Option[FileAccessRights.Value] = Option.empty
                                ) extends FileMetadata(filepath, mimeType)
  case class AVFileMetadata(override val filepath: File,
                            override val mimeType: MimeType,
                            vocabulary: AudioVideo,
                            title: String,
                            accessibleTo: FileAccessRights.Value,
                            subtitles: Set[Subtitles]
                           ) extends FileMetadata(filepath, mimeType)

  private lazy val defaultAccessibility: FileAccessRights.Value = {
    FileAccessRights.accessibleTo(deposit.profile.accessright)
  }

  private def getFileMetadata(file: File): Try[FileMetadata] = {
    def mkDefaultFileMetadata(m: MimeType): FileMetadata = {
      deposit.files.get(file)
        .map(fd => DefaultFileMetadata(file, m, fd.title, fd.accessibility))
        .getOrElse(DefaultFileMetadata(file, m))
    }

    def mkAVFileMetadata(m: MimeType, voca: AudioVideo): AVFileMetadata = {
      val subtitles = deposit.audioVideo.avFiles.getOrElse(file, Set.empty)

      deposit.files.get(file)
        .map(fd => {
          val title = fd.title.getOrElse(file.name)
          val accessibility = fd.accessibility.getOrElse(defaultAccessibility)
          AVFileMetadata(file, m, voca, title, accessibility, subtitles)
        })
        .getOrElse(AVFileMetadata(file, m, voca, file.name, defaultAccessibility, subtitles))
    }

    getMimeType(file).map {
      case mimeType if mimeType startsWith "audio" => mkAVFileMetadata(mimeType, Audio)
      case mimeType if mimeType startsWith "video" => mkAVFileMetadata(mimeType, Video)
      case mimeType => mkDefaultFileMetadata(mimeType)
    }
  }

  private lazy val fileMetadata: Try[Seq[FileMetadata]] = Try {
    val depositDir = multiDepositDir(deposit.depositId)
    if (depositDir.exists) {
      depositDir.listRecursively
        .withFilter(!_.isDirectory)
        .map(getFileMetadata)
        .toSeq
        .collectResults
    }
    else // if the deposit does not contain any data
      Success { List.empty }
  }.flatten

  /**
   * Verifies whether all preconditions are met for this specific action.
   * All files referenced in the instructions are checked for existence.
   *
   * @return `Success` when all preconditions are met, `Failure` otherwise
   */
  override def checkPreconditions: Try[Unit] = {
    def checkSFColumnsIfDepositContainsAVFiles(mimetypes: Seq[FileMetadata]): Try[Unit] = {
      val avFiles = mimetypes.collect { case fmd: AVFileMetadata => fmd.filepath }

      (deposit.audioVideo.springfield.isDefined, avFiles.isEmpty) match {
        case (true, false) | (false, true) => Success(())
        case (true, true) =>
          Failure(ActionException(deposit.row,
            "Values found for these columns: [SF_DOMAIN, SF_USER, SF_COLLECTION]\n" +
              "cause: these columns should be empty because there are no audio/video files " +
              "found in this deposit"))
        case (false, false) =>
          Failure(ActionException(deposit.row,
            "No values found for these columns: [SF_USER, SF_COLLECTION]\n" +
              "cause: these columns should contain values because audio/video files are " +
              s"found:\n${ avFiles.map(filepath => s" - $filepath").mkString("\n") }"))
      }
    }

    def checkEitherVideoOrAudio(mimetypes: Seq[FileMetadata]): Try[Unit] = {
      mimetypes.collect { case fmd: AVFileMetadata => fmd.vocabulary }.distinct match {
        case Nil | Seq(_) => Success(())
        case _ => Failure(ActionException(deposit.row,
          "Found both audio and video in this dataset. Only one of them is allowed."))
      }
    }

    for {
      fmds <- fileMetadata
      _ <- checkSFColumnsIfDepositContainsAVFiles(fmds)
      _ <- checkEitherVideoOrAudio(fmds)
    } yield ()
  }

  override def execute(): Try[Unit] = {
    depositToFileXml
      .map(stagingFileMetadataFile(deposit.depositId).writeXml(_))
      .recoverWith {
        case NonFatal(e) => Failure(ActionException(deposit.row, s"Could not write file meta data: $e", e))
      }
  }

  def depositToFileXml: Try[Elem] = {
    fileMetadata.map(fileXmls(_) match {
      case Nil => <files
        xmlns:dcterms="http://purl.org/dc/terms/"
        xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation={"http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd " +
          "http://easy.dans.knaw.nl/schemas/bag/metadata/files/ http://easy.dans.knaw.nl/schemas/bag/metadata/files/files.xsd"}/>
      case files => <files
        xmlns:dcterms="http://purl.org/dc/terms/"
        xmlns="http://easy.dans.knaw.nl/schemas/bag/metadata/files/"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation={"http://purl.org/dc/terms/ http://dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd " +
          "http://easy.dans.knaw.nl/schemas/bag/metadata/files/ http://easy.dans.knaw.nl/schemas/bag/metadata/files/files.xsd"}>{files}</files>
    })
  }

  private def fileXmls(fmds: Seq[FileMetadata]): Seq[Elem] = {
    fmds.map {
      case av: AVFileMetadata => avFileXml(av)
      case fmd: DefaultFileMetadata => defaultFileXml(fmd)
    }
  }

  private def defaultFileXml(fmd: DefaultFileMetadata): Elem = {
    <file filepath={s"data/${ multiDepositDir(deposit.depositId).relativize(fmd.filepath) }"}>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      {fmd.title.map(title => <dcterms:title>{title}</dcterms:title>).getOrElse(NodeSeq.Empty)}
      {fmd.accessibleTo.map(act => <accessibleToRights>{act}</accessibleToRights>).getOrElse(NodeSeq.Empty)}
    </file>
  }

  private def avFileXml(fmd: AVFileMetadata): Elem = {
    <file filepath={s"data/${ multiDepositDir(deposit.depositId).relativize(fmd.filepath) }"}>
      <dcterms:type>{fmd.vocabulary.vocabulary}</dcterms:type>
      <dcterms:format>{fmd.mimeType}</dcterms:format>
      <dcterms:title>{fmd.title}</dcterms:title>
      <accessibleToRights>{fmd.accessibleTo}</accessibleToRights>
      {fmd.subtitles.map(subtitleXml)}
    </file>
  }

  private def subtitleXml(subtitle: Subtitles): Elem = {
    val filepath = multiDepositDir(deposit.depositId).relativize(subtitle.file)

    subtitle.language
      .map(lang => <dcterms:relation xml:lang={lang}>{s"data/$filepath"}</dcterms:relation>)
      .getOrElse(<dcterms:relation>{s"data/$filepath"}</dcterms:relation>)
  }
}

object AddFileMetadataToDeposit {
  private val tika = new Tika
  type MimeType = String

  /**
   * Identify the mimeType of a path.
   *
   * @param file the path to identify
   * @return the mimeType of the path if the identification was successful; `Failure` otherwise
   */
  def getMimeType(file: File): Try[MimeType] = Try {
    tika.detect(file.path)
  }
}

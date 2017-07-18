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
package nl.knaw.dans.easy.multideposit

import javax.naming.directory.{ Attributes, BasicAttribute, BasicAttributes }

import better.files._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }
import scala.xml.transform.{ RewriteRule, RuleTransformer }
import scala.xml.{ Elem, Node, NodeSeq, XML }

class BlackBoxSpec extends UnitSpec with BeforeAndAfter with MockFactory with CustomMatchers {

  private val formatsFile: File = testDir / "formats.txt"
  private val allfields = testDir / "md" / "allfields"
  private val invalidCSV = testDir / "md" / "invalidCSV"

  before {
    File(getClass.getResource("/debug-config/formats.txt").toURI).copyTo(formatsFile)
    File(getClass.getResource("/allfields/input").toURI).copyTo(allfields)
    File(getClass.getResource("/invalidCSV/input").toURI).copyTo(invalidCSV)
  }

  "allfields" should "succeed in transforming the input into a bag" in {
    assume(System.getProperty("user.name") != "travis",
      "this test does not work on travis, because we don't know the group that we can use for this")

    val ldap = mock[Ldap]
    implicit val settings = Settings(
      multidepositDir = allfields,
      stagingDir = testDir / "sd",
      outputDepositDir = testDir / "od",
      datamanager = "easyadmin",
      depositPermissions = DepositPermissions("rwxrwx---", "admin"),
      formatsFile = formatsFile,
      ldap = ldap
    )
    val expectedOutputDir = File(getClass.getResource("/allfields/output").toURI)
    settings.outputDepositDir.createDirectories()

    def createDatamanagerAttributes: BasicAttributes = {
      new BasicAttributes() {
        put("dansState", "ACTIVE")
        put(new BasicAttribute("easyRoles") {
          add("USER")
          add("ARCHIVIST")
        })
        put("mail", "FILL.IN.YOUR@VALID-EMAIL.NL")
      }
    }

    (ldap.query(_: String)(_: Attributes => Attributes)) expects(settings.datamanager, *) returning Success(Seq(createDatamanagerAttributes))
    (ldap.query(_: String)(_: Attributes => Boolean)) expects("user001", *) repeat 4 returning Success(Seq(true))

    Main.run shouldBe a[Success[_]]

    val expectedDataContent = Map(
      "ruimtereis01" -> Set("ruimtereis01_verklaring.txt", "path/", "to/", "a/", "random/",
        "video/", "hubble.mpg", "reisverslag/", "centaur.mpg", "centaur.srt",
        "centaur-nederlands.srt", "deel01.docx", "deel01.txt", "deel02.txt", "deel03.txt"),
      "ruimtereis02" -> Set("hubble-wiki-en.txt", "hubble-wiki-nl.txt", "path/", "to/",
        "images/", "Hubble_01.jpg", "Hubbleshots.jpg"),
      "ruimtereis03" -> Set.empty,
      "ruimtereis04" -> Set("Quicksort.hs", "path/", "to/", "a/", "random/", "file/",
        "file.txt", "sound/", "chicken.mp3")
    )

    for (bagName <- Seq("ruimtereis01", "ruimtereis02", "ruimtereis03", "ruimtereis04")) {
      // TODO I'm not happy with this way of testing the content of each file, especially with ignoring specific lines,
      // but I'm in a hurry, so I'll think of a better way later
      val bag = settings.outputDepositDir / s"allfields-$bagName" / "bag"
      val expBag = expectedOutputDir / s"input-$bagName" / "bag"

      bag.list.map(_.name).toList should contain only(
        "bag-info.txt",
        "bagit.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt",
        "data",
        "metadata")

      val bagInfo = bag / "bag-info.txt"
      val expBagInfo = expBag / "bag-info.txt"
      bagInfo.lines should contain allElementsOf expBagInfo.lines.filterNot(_ contains "Bagging-Date").toSeq

      bag / "bagit.txt" === expBag / "bagit.txt"
      bag / "manifest-sha1.txt" === expBag / "manifest-sha1.txt"

      val tagManifest = bag / "tagmanifest-sha1.txt"
      val expTagManifest = expBag / "tagmanifest-sha1.txt"
      tagManifest.lines should contain allElementsOf expTagManifest.lines.filterNot(_ contains "bag-info.txt").filterNot(_ contains "manifest-sha1.txt").toSeq

      val dataDir = bag / "data"
      dataDir.toJava should exist
      dataDir.listRecursively.map {
        case file if file.isDirectory => file.name + "/"
        case file => file.name
      }.toList should contain theSameElementsAs expectedDataContent(bagName)

      (bag / "metadata").list.map(_.name).toList should contain only("dataset.xml", "files.xml")

      val datasetXml = bag / "metadata" / "dataset.xml"
      val expDatasetXml = expBag / "metadata" / "dataset.xml"
      val datasetTransformer = removeElemByName("available")
      datasetTransformer.transform(XML.loadFile(datasetXml.toJava)) should equalTrimmed (datasetTransformer.transform(XML.loadFile(expDatasetXml.toJava)))

      val filesXml = bag / "metadata" / "files.xml"
      val expFilesXml = expBag / "metadata" / "files.xml"
      (XML.loadFile(filesXml.toJava) \ "files").toSet should equalTrimmed((XML.loadFile(expFilesXml.toJava) \ "files").toSet)

      val props = settings.outputDepositDir / s"allfields-$bagName" / "deposit.properties"
      val expProps = expectedOutputDir / s"input-$bagName" / "deposit.properties"
      props.lines should contain allElementsOf expProps.lines.filterNot(_ startsWith "#").filterNot(_ contains "bag-store.bag-id").toSeq
    }
  }

  "invalidCSV" should "fail in the parser step and return a report of the errors" in {
    implicit val settings = Settings(
      multidepositDir = invalidCSV
    )

    inside(Main.run) {
      case Failure(ParserFailedException(report, _)) =>
        report.lines.toSeq should contain inOrderOnly(
          "CSV failures:",
          " - row 2: Only one row is allowed to contain a value for the column 'DEPOSITOR_ID'. Found: [user001, invalid-user]",
          " - row 2: DDM_CREATED value 'invalid-date' does not represent a date",
          " - row 2: Only one row is allowed to contain a value for the column 'DDM_ACCESSRIGHTS'. Found: [OPEN_ACCESS, GROUP_ACCESS]",
          " - row 2: Value 'random test data' is not a valid type",
          " - row 2: Value 'NL' is not a valid value for DC_LANGUAGE",
          " - row 2: Missing value for: SF_USER",
          " - row 2: AV_FILE 'path/to/audiofile/that/does/not/exist.mp3' does not exist",
          " - row 3: DDM_AVAILABLE value 'invalid-date' does not represent a date",
          " - row 3: Missing value for: DC_IDENTIFIER",
          " - row 3: Value 'encoding=UTF-8' is not a valid value for DC_LANGUAGE",
          " - row 3: No value is defined for AV_FILE, while some of [AV_FILE_TITLE, AV_SUBTITLES, AV_SUBTITLES_LANGUAGE] are defined",
          "Due to these errors in the 'instructions.csv', nothing was done."
        )
    }
  }

  def removeElemByName(label: String) = new RuleTransformer(new RewriteRule {
    override def transform(n: Node): Seq[Node] = {
      n match {
        case e: Elem if e.label == label => NodeSeq.Empty
        case e => e
      }
    }
  })
}

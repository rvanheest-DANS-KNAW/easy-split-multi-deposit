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

import nl.knaw.dans.easy.multideposit.model.{ Deposit, DepositId }
import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.Success

class AddBagToDepositSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = testDir / "md",
    stagingDir = testDir / "sd"
  )

  val depositId = "ruimtereis01"
  val file1Text = "abcdef"
  val file2Text = "defghi"
  val file3Text = "ghijkl"
  val file4Text = "jklmno"
  val file5Text = "mnopqr"
  val deposit: Deposit = testDeposit1

  before {
    (multiDepositDir(depositId) / "file1.txt").createIfNotExists(createParents = true).write(file1Text)
    (multiDepositDir(depositId) / "folder1" / "file2.txt").createIfNotExists(createParents = true).write(file2Text)
    (multiDepositDir(depositId) / "folder1" / "file3.txt").createIfNotExists(createParents = true).write(file3Text)
    (multiDepositDir(depositId) / "folder2" / "file4.txt").createIfNotExists(createParents = true).write(file4Text)
    (multiDepositDir("ruimtereis02") / "folder3" / "file5.txt").createIfNotExists(createParents = true).write("file5Text")

    stagingBagDir(depositId).createDirectories()
  }

  "execute" should "succeed given the current setup" in {
    AddBagToDeposit(deposit).execute shouldBe a[Success[_]]
  }

  it should "create a bag with the files from ruimtereis01 in it and some meta-files around" in {
    AddBagToDeposit(deposit).execute shouldBe a[Success[_]]

    val root = stagingBagDir(depositId)
    root.toJava should exist
    root.walk().map {
      case file if file.isDirectory => file.name + "/"
      case file => file.name
    }.toList should contain theSameElementsAs
      List("bag/",
        "bag-info.txt",
        "bagit.txt",
        "data/",
        "file1.txt",
        "folder1/",
        "file2.txt",
        "file3.txt",
        "folder2/",
        "file4.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")
    stagingBagDataDir(depositId).toJava should exist
  }

  it should "preserve the file content after making the bag" in {
    AddBagToDeposit(deposit).execute shouldBe a[Success[_]]

    val root = stagingBagDataDir(depositId)
    (root / "file1.txt").contentAsString shouldBe file1Text
    (root / "folder1" / "file2.txt").contentAsString shouldBe file2Text
    (root / "folder1" / "file3.txt").contentAsString shouldBe file3Text
    (root / "folder2" / "file4.txt").contentAsString shouldBe file4Text
  }

  it should "create a bag with no files in data when the input directory does not exist" in {
    implicit val settings = Settings(
      multidepositDir = testDir / "md-empty",
      stagingDir = testDir / "sd"
    )

    val outputDir = stagingBagDir(depositId)
    outputDir.createDirectories()
    outputDir.toJava should exist

    multiDepositDir(depositId).toJava should not(exist)

    AddBagToDeposit(deposit)(settings).execute shouldBe a[Success[_]]

    stagingDir(depositId).toJava should exist
    stagingBagDataDir(depositId).toJava should exist
    stagingBagDataDir(depositId).listRecursively.filter(!_.isDirectory) shouldBe empty
    stagingBagDir(depositId)
      .listRecursively
      .withFilter(!_.isDirectory)
      .map(_.name)
      .toList should contain theSameElementsAs
      List("bag-info.txt",
        "bagit.txt",
        "manifest-sha1.txt",
        "tagmanifest-sha1.txt")

    val root = stagingBagDir(depositId)
    (root / "manifest-sha1.txt").contentAsString shouldBe empty
    (root / "tagmanifest-sha1.txt").contentAsString should include("bag-info.txt")
    (root / "tagmanifest-sha1.txt").contentAsString should include("bagit.txt")
    (root / "tagmanifest-sha1.txt").contentAsString should include("manifest-sha1.txt")
  }

  it should "contain the date-created in the bag-info.txt" in {
    AddBagToDeposit(deposit).execute() shouldBe a[Success[_]]

    val bagInfo = stagingBagDir(depositId) / "bag-info.txt"
    bagInfo.toJava should exist

    bagInfo.contentAsString should include("Created")
  }

  it should "contain the correct checksums in its manifest file" in {
    AddBagToDeposit(deposit).execute() shouldBe a[Success[_]]

    verifyChecksums(depositId, "manifest-sha1.txt")
  }

  it should "contain the correct checksums in its tagmanifest file" in {
    AddBagToDeposit(deposit).execute() shouldBe a[Success[_]]

    verifyChecksums(depositId, "tagmanifest-sha1.txt")
  }

  def verifyChecksums(depositId: DepositId, manifestFile: String): Unit = {
    val root = stagingBagDir(depositId)
    (root / manifestFile).lineIterator
      .map(_.split("  "))
      .foreach {
        case Array(sha1, file) => (root / file).sha1.toLowerCase shouldBe sha1
        case line => fail(s"unexpected line detected: ${ line.mkString("  ") }")
      }
  }
}

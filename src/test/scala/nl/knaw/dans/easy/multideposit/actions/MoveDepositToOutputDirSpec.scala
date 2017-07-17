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

import better.files.File
import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }

class MoveDepositToOutputDirSpec extends UnitSpec with BeforeAndAfter {

  implicit val settings = Settings(
    multidepositDir = testDir./("input"),
    stagingDir = testDir./("sd"),
    outputDepositDir = testDir./("dd")
  )

  before {
    // create stagingDir content
    val baseDir = settings.stagingDir
    baseDir.createDirectory()
    baseDir.toJava should exist

    File(getClass.getResource("/allfields/output/input-ruimtereis01").toURI)
      .copyTo(stagingDir("ruimtereis01"))
    File(getClass.getResource("/allfields/output/input-ruimtereis02").toURI)
      .copyTo(stagingDir("ruimtereis02"))

    stagingDir("ruimtereis01").toJava should exist
    stagingDir("ruimtereis02").toJava should exist
  }

  "checkPreconditions" should "verify that the deposit does not yet exist in the outputDepositDir" in {
    MoveDepositToOutputDir(1, "ruimtereis01").checkPreconditions shouldBe a[Success[_]]
  }

  it should "fail if the deposit already exists in the outputDepositDir" in {
    val depositId = "ruimtereis01"
    stagingDir(depositId).copyTo(outputDepositDir(depositId))
    outputDepositDir(depositId).toJava should exist

    inside(MoveDepositToOutputDir(1, depositId).checkPreconditions) {
      case Failure(ActionException(1, msg, _)) => msg should include(s"The deposit for dataset $depositId already exists")
    }
  }

  "execute" should "move the deposit to the outputDepositDirectory" in {
    val depositId = "ruimtereis01"
    MoveDepositToOutputDir(1, depositId).execute() shouldBe a[Success[_]]

    stagingDir(depositId).toJava shouldNot exist
    outputDepositDir(depositId).toJava should exist

    stagingDir("ruimtereis02").toJava should exist
    outputDepositDir("ruimtereis02").toJava shouldNot exist
  }

  it should "only move the one deposit to the outputDepositDirectory, not other deposits in the staging directory" in {
    val depositId = "ruimtereis01"
    MoveDepositToOutputDir(1, depositId).execute() shouldBe a[Success[_]]

    stagingDir("ruimtereis02").toJava should exist
    outputDepositDir("ruimtereis02").toJava shouldNot exist
  }
}

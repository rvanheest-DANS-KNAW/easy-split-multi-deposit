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

import java.nio.file.FileSystemException
import java.nio.file.attribute.{ PosixFilePermission, UserPrincipalNotFoundException }

import nl.knaw.dans.easy.multideposit.{ Settings, UnitSpec, _ }
import org.scalatest.BeforeAndAfter

import scala.util.{ Failure, Success }

class SetDepositPermissionsSpec extends UnitSpec with BeforeAndAfter {

  private val (user, userGroup, unrelatedGroup) = {
    import scala.sys.process._

    // don't hardcode users and groups, since we don't know what we have on travis
    val user = System.getProperty("user.name")
    val allGroups = "cut -d: -f1 /etc/group".!!.split("\n").filterNot(_ startsWith "#").toList
    val userGroups = s"id -Gn $user".!!.split(" ").toList

    (userGroups, allGroups.diff(userGroups)) match {
      case (ug :: _, diff :: _) => (user, ug, diff)
      case (Nil, _) => throw new AssertionError("no suitable user group found")
      case (_, Nil) => throw new AssertionError("no suitable unrelated group found")
    }
  }

  implicit val settings = Settings(
    multidepositDir = testDir / "md",
    stagingDir = testDir / "sd",
    depositPermissions = DepositPermissions("rwxrwx---", userGroup)
  )

  private val depositId = "ruimtereis01"

  private val base = stagingDir(depositId)
  private val folder1 = base / "folder1"
  private val folder2 = base / "folder2"
  private val file1 = base / "file1.txt"
  private val file2 = folder1 / "file2.txt"
  private val file3 = folder1 / "file3.txt"
  private val file4 = folder2 / "file4.txt"
  private val filesAndFolders = List(base, folder1, folder2, file1, file2, file3, file4)

  before {
    base.createDirectories()
    folder1.createDirectories()
    folder2.createDirectories()

    file1.write("abcdef")
    file2.write("defghi")
    file3.write("ghijkl")
    file4.write("jklmno")

    for (file <- filesAndFolders) {
      file.toJava shouldBe readable
      file.toJava shouldBe writable
    }
  }

  "setFilePermissions" should "set the permissions of each of the files and folders to the correct permissions" in {
    assume(user != "travis",
      "this test does not work on travis, because we don't know the group that we can use for this")

    SetDepositPermissions(1, depositId).execute() shouldBe a[Success[_]]

    for (file <- filesAndFolders) {
      file.permissions should {
        have size 6 and contain only(
          PosixFilePermission.OWNER_READ,
          PosixFilePermission.OWNER_WRITE,
          PosixFilePermission.OWNER_EXECUTE,
          PosixFilePermission.GROUP_READ,
          PosixFilePermission.GROUP_WRITE,
          PosixFilePermission.GROUP_EXECUTE
        ) and contain noneOf(
          PosixFilePermission.OTHERS_READ,
          PosixFilePermission.OTHERS_WRITE,
          PosixFilePermission.OTHERS_EXECUTE
        )
      }
      file.groupName shouldBe userGroup
    }
  }

  it should "fail if the group name does not exist" in {
    implicit val settings = Settings(
      multidepositDir = testDir / "md",
      stagingDir = testDir / "sd",
      depositPermissions = DepositPermissions("rwxrwx---", "non-existing-group-name")
    )

    inside(SetDepositPermissions(1, depositId)(settings).execute()) {
      case Failure(ActionException(1, msg, _: UserPrincipalNotFoundException)) => msg shouldBe "Group non-existing-group-name could not be found"
    }
  }

  it should "fail if the access permissions are invalid" in {
    implicit val settings = Settings(
      multidepositDir = testDir / "md",
      stagingDir = testDir / "sd",
      depositPermissions = DepositPermissions("abcdefghi", "admin")
    )

    inside(SetDepositPermissions(1, depositId)(settings).execute()) {
      case Failure(ActionException(1, msg, _: IllegalArgumentException)) => msg shouldBe "Invalid privileges (abcdefghi)"
    }
  }

  it should "fail if the user is not part of the given group" in {
    implicit val settings = Settings(
      multidepositDir = testDir / "md",
      stagingDir = testDir / "sd",
      depositPermissions = DepositPermissions("rwxrwx---", unrelatedGroup)
    )

    inside(SetDepositPermissions(1, depositId)(settings).execute()) {
      case Failure(ActionException(1, msg, _: FileSystemException)) => msg should include(s"Not able to set the group to $unrelatedGroup")
    }
  }
}

// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class LtHash16Test extends AnyWordSpec with BaseTest {

  "LtHash16" should {
    // Golden test taken from
    // https://github.com/facebook/folly/blob/993de57926e7b17306ac9c5c46781a15d1b04414/folly/experimental/crypto/test/LtHashTest.cpp
    "pass the golden test" in {
      val h = LtHash16()
      h.add("a".getBytes)
      h.add("b".getBytes)
      h.add("hello".getBytes)
      h.remove("b".getBytes)
      val computed = hexString(h.get())
      val expected =
        "353cae6169e519eb9cf80edd2c5b33810276227e77a09030e3ac3b00299c9716c6b592b262b2b05ad82db539f23fc03baa1ffacc9704fe078219307c02c0f501c810895c19a771934855d091e30db8eba564596f071400fcca93b69115055c55e0b333b5583ec0068a219289b557be5b24cfa679ae8e20b9084c77eadab966e4f94239d5f671371aa17c41f0510aaaeb6e28fed0eb37b57c5ff8f6c64a0395ddb32d2948abee9ae84930ee0d43d015b2f577cadb558eef33e715f349114c1937817ff26b606f1f33a1f3b4a72eaa3b24573a78d06b315857a8295675ec2bfc9897b644f60d401c4315bea8a6ad410f77e3969aaa032d31526df0c271665647c98f1e4d3946b659e47f45480c3eac9b0e0b742501595b24d5362d3f6f4ba8a4fcda7d87951ade9ec184a45c2fd5bff5282835c29071551e96d940c3ed19bb3124c3b37080dc3c80bc22f61b431195b9489bed3244e0e522bf8f8c752145b01ee47701085ffa1238f3a1d5e778052b393330fff8b586d9399cced75d4d15697f9015174d3302d97b1cc55ae20cdb573d4061d2940b213a35808122e7d55bd53e2c9ba1779c8a19532ff1e65a440e871f96e086dce6693efba86e033f7e3b04069f9eeccc0f5c5947af0b04f5528be1b57bba0912eeb52fdd11f0cac0e40ae641bbc40207188adbfe13463c880e84016476facae56f7f6de26e7f508a277a409988aabec7f9bb552000e3f7a44f51ec5c7c98979a227403464797a06fae0d7aa951bb429cb9df4ed65a430a98e0c88f7d4e47e1256f17c4b126f05b885154507b3b80a2a1b6e1f43eea48b4b93cab0622bd002a25dd5d1b69fe05c11619837eba6edfac493d663409f5ce82762584205fe49e8f718fbc5a92823cd9a17c1c9a07ce9f2535c918c6ee0f0729b67eb0be8b5e0edc990260679fdf5a9991a6d62ec1d72f5e5a478dbf0e5cbd1703daf5f170411d0d7aca4921cb644ec1d86e02711d09359b0f2b45a5b9fe57e122add8b5ae27aaeb44aa77a9fe187a67ea7447b27d02b4bf41fc5024350dc8838fb8f977535ba4481569a74d90306e0c9979a9d149be950223d1ca5d425b9ec281ee3884d8e8a1ad0d00504f0f57ff35e0ee33d184f35fd28dfa348686fb926da95597fb947acc509a5cb7cfe1eeb33dfdf9b4b384346c862cfb198f6948a6f6d53a74848043c8b647076b0a90151bd40c58d32434ebf549aa92f4a5b7581b7ec6821ca3485cf8e2a6ce0f5e204ed5a92c84618c2828e5c6f222ec3c48e37dcada7ce28bba5c09740170d32aa004b43cde46d45f9912528a3a7a7f30fb6019548dd174b4d7b0bafb232920b972362db4d863a5e0a9e30a041ecb874a7acbd378ccb11ffbffcd086ad797be5b4de07859d0b1fb3e4835a84ea224940482a3849cf392528dfcf8920d4b4bfc40606e852d85b7bfd1f2723214969dab6adfb8c26dc5f51b1b043b8a25df1eadd90d1a23240b735943841ae4e13564ddb6f0f7dcac1db82a34ab9ca042f8c4690727c7a0fac98c10dac065a57dff8010e9d49ba3b801622e8b786bb44079ceecf61ff7cc07be8672c647b525ffea7c3fab95d40d9d36e220bb3a5292880faf05a8dd94e60a4ff0ccfc124d2dca03a85d0864bfa28cddb7bdcc83ff717239dae979596691b6e3062068e6ea442ebd354bc653b0e5b750bcbfaec275c77ab82bd3452e4776734df686d6bae946855a4659dd3566f48d0879a00c06a7ce81c0f234e0203ce68ffc9434f3f10281d76110887a4b460514f761b517f1d151d88724160fbeff7f69a5a23eae2bf48916ad55c084b908d955519a67096b94638fa10d8d153a60d0c44f2d9148ad549fb1e64ac423aac1fdc754bc44a69573578c6b881bca177698e68d6ffdc2d7d89469f2e1039e8e3b955581a56c15519590b65bd9bcc3b3b1a95d1d484c2585ebdfd8a15c737b436456934d9b8439d92d1212bf8799028780d9f35d208c093ba6506aff74979faa10fa807398e8fb769be070318caee6b4f5091d8d9254656d0a1e838ba73ed0f0c8e8d4a0d19f9e91340578baa7ce5aec9f73f8e26db9273c544f11d6b8e5e142f4a8ad70a9e21c3dc2c7b4403073c4722e0af775f98c37ae0645e1829dec574de3108f62965a5354aaa7695c1e4feab1fc8ccf9a5e2a7ed0758e411ea9ea25f4f659a36cc5aa0bed2a9ce4518cd1aa1b4ed94c2d62596059d20cd948a058b78ef9ad3c9e7c7c9fd433c42701aad7aff74fb14ea39812c3e68b6ca8585432ecd53a7dfeece8e6a73b0ecddabc8c9da37b140adee6308c540bedbcf77d49762e7efaeededf51966503315fb287b69a08854ec58fd41c2f214c3273cc48bc71718b801c27936c7fd339b7f78c2eda6835e7d532ef6496cbbc7b018cc48ed49e33c16e16d2bdc98f47f376208770b5d5b6e789b30ca55ad4e8cd09b6bd90b66e8d4abfd0fbc3e98fc28f3913e476161d0f0f7477d3ca066adce7567d1af90dc3415970199ada286e22ea90892da107c34d3745c5d5d3f290fbd2d0b64942117955e94b343517d76959f0764216ce27bc33e772fefe4a4820315d67b43302f73e8002cc6144c3f3ad66c307eb2f9e0192a00da9ddf262b600dd0a49721da25ca71997d17c3441cd9588c5469f6927966d06676f89243387f01ebd2094beac0716a2e486d85524c51ba2898abb8b8df3a169f93fe6333f4f6a868738969905ba55176f1b8055d19749c0c5122ab1eaf34b0eb458fc10a656811fe4a7bb588eac3450b6d61c7f634588996ef2d903478cde206f58c1a93069c7df80a28394d05e9a8ed99b5312e9cbec0a2dba2e3e3e24e854798e9dc8c09922fadb987e945a765e2f614993f2b56055481e3702371d4eb86c872ca65269125be61d86"
      computed shouldBe expected
    }

    "be commutative" in {
      val h1 = LtHash16()
      val h2 = LtHash16()
      h1.add("a".getBytes)
      h1.add("b".getBytes)
      h2.add("b".getBytes)
      h2.add("a".getBytes)
      h1.get() shouldBe h2.get()
    }

    "removal should be the inverse of addition" in {
      val h = LtHash16()
      val emptyBytes = h.getByteString()
      val abc = "abc".getBytes()
      h.add(abc)
      val abcBytes = h.getByteString()
      val defg = "defg".getBytes()
      h.add(defg)
      h.remove(defg)
      h.getByteString() shouldBe abcBytes
      h.remove(abc)
      h.getByteString() shouldBe emptyBytes
    }

    "correctly determine empty commitment bytes" in {
      val h = LtHash16()
      LtHash16.isNonEmptyCommitment(h.getByteString()) shouldBe false
    }

    "correctly determine non-empty commitment bytes" in {
      val h = LtHash16()
      h.add("123".getBytes())
      LtHash16.isNonEmptyCommitment(h.getByteString()) shouldBe true
    }
  }

  def hexString(buf: Array[Byte]): String = buf.map("%02X" format _).mkString.toLowerCase
}

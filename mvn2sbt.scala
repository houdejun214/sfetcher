object scala {
  val version = "2.11"
}

val xml =
  <dependencies>
  <dependency>
    <groupId>org.scalanlp</groupId>
    <artifactId>scalala_${scala.version}</artifactId>
    <version>0.3.1</version>
  </dependency>


  <!-- Apache Commons -->
  <dependency>
    <groupId>commons-io</groupId>
    <artifactId>commons-io</artifactId>
    <version>1.4</version>
  </dependency>
  <dependency>
    <groupId>commons-lang</groupId>
    <artifactId>commons-lang</artifactId>
    <version>2.4</version>
  </dependency>

  <dependency>
    <groupId>joda-time</groupId>
    <artifactId>joda-time</artifactId>
    <version>1.6</version>
  </dependency>

  <dependency>
    <groupId>joda-time</groupId>
    <artifactId>joda-time-hibernate</artifactId>
    <version>1.1</version>
    <exclusions>
      <exclusion>
        <groupId>cglib</groupId>
        <artifactId>cglib-full</artifactId>
      </exclusion>
      <exclusion>
        <groupId>ehcache</groupId>
        <artifactId>ehcache</artifactId>
      </exclusion>
      <exclusion>
        <groupId>org.hibernate</groupId>
        <artifactId>hibernate</artifactId>
      </exclusion>
      <exclusion>
        <groupId>antlr</groupId>
        <artifactId>antlr</artifactId>
      </exclusion>
    </exclusions>
  </dependency>

  <dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-library</artifactId>
    <version>${scala.version}</version>
  </dependency>
  <dependency>
    <groupId>org.scala-lang</groupId>
    <artifactId>scala-compiler</artifactId>
    <version>${scala.version}</version>
  </dependency>
  <dependency>
    <groupId>com.googlecode.scalaz</groupId>
    <artifactId>scalaz-core_${scala.version}</artifactId>
    <version>5.0-SNAPSHOT</version>
  </dependency>

  <dependency>
    <groupId>org.scala-tools.testing</groupId>
    <artifactId>specs_${scala.version}</artifactId>
    <version>1.6.5-SNAPSHOT</version>
  </dependency>
  <dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.7</version>
    <optional>true</optional>
  </dependency>

  <dependency>
    <groupId>org.scala-tools.testing</groupId>
    <artifactId>test-interface</artifactId>
    <version>0.5</version>
    <optional>true</optional>
  </dependency>
  <dependency>
    <groupId>org.scalatest</groupId>
    <artifactId>scalatest</artifactId>
    <version>1.0.1-for-scala-${scala.version}-SNAPSHOT</version>
  </dependency>
  <dependency>
    <groupId>org.easymock</groupId>
    <artifactId>easymock</artifactId>
    <version>2.5.1</version>
  </dependency>
  <dependency>
    <groupId>org.easymock</groupId>
    <artifactId>easymockclassextension</artifactId>
    <version>2.4</version>
  </dependency>
  <dependency>
    <groupId>org.scala-tools.testing</groupId>
    <artifactId>scalacheck_${scala.version}</artifactId>
    <version>1.7</version>
  </dependency>
  <dependency>
    <groupId>org.jmock</groupId>
    <artifactId>jmock</artifactId>
    <version>2.5.1</version>
  </dependency>
  <dependency>
    <groupId>org.jmock</groupId>
    <artifactId>jmock-legacy</artifactId>
    <version>2.5.1</version>
  </dependency>
  <dependency>
    <groupId>org.mockito</groupId>
    <artifactId>mockito-all</artifactId>
    <version>1.8.4</version>
  </dependency>
  <dependency>
    <groupId>cglib</groupId>
    <artifactId>cglib</artifactId>
    <version>2.1_3</version>
  </dependency>
  <dependency>
    <groupId>org.objenesis</groupId>
    <artifactId>objenesis</artifactId>
    <version>1.0</version>
  </dependency>


  <dependency>
    <groupId>net.objectlab.kit.datecalc</groupId>
    <artifactId>datecalc-joda</artifactId>
    <version>1.1.0</version>
  </dependency>
  <dependency>
    <groupId>net.objectlab.kit.datecalc</groupId>
    <artifactId>datecalc-common</artifactId>
    <version>1.1.0</version>
  </dependency>
  <dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-math</artifactId>
    <version>2.0</version>
  </dependency>
</dependencies>


val data: Seq[(String, String, String)] = (xml \ "dependency") map { d =>
  val groupId = d \ "groupId" text
  val artifactId = d \ "artifactId" text
  val versionNum = d \ "version" text

  (groupId, artifactId, versionNum)
}

val CrossBuildArtifact = """([\w-]+)_\$SCALA_VERSION\$""".r

def dep(a: String, g: String, v: String, cross: Boolean) = {
  val sep = if (cross) "%%" else "%"
  val ident = a.split("-").map(_.capitalize).mkString
  """val %s = "%s" %s "%s" %% "%s" """ format (ident, g, sep, a, v)
}

val m = data map {
  case (g, CrossBuildArtifact(a), v) => dep(a, g, v, true)
  case (g, a, v) => dep(a, g, v, false)
} mkString("\n")
println(m)
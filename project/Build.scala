import com.typesafe.sbt.SbtPgp
import sbt.Keys._
import sbt.{addCompilerPlugin, _}

/** Adds common settings automatically to all subprojects */
object Build extends AutoPlugin {

  object autoImport {
    val org = "com.sksamuel.avro4s"
    val AvroVersion = "1.9.0"
    val Log4jVersion = "1.2.17"
    val ScalatestVersion = "3.0.8"
    val Slf4jVersion = "1.7.28"
    val Json4sVersion = "3.6.7"
    val CatsVersion = "2.0.0-RC2"
    val ShapelessVersion = "2.3.3"
    val RefinedVersion = "0.9.9"
    val MagnoliaVersion = "0.11.0"
  }

  import autoImport._

  def isTravis = System.getenv("TRAVIS") == "true"
  def travisBuildNumber = System.getenv("TRAVIS_BUILD_NUMBER")

  override def trigger = allRequirements
  override def projectSettings = publishingSettings ++ Seq(
    organization := org,
    scalaVersion := "2.12.8",
    crossScalaVersions := Seq("2.12.8", "2.13.0"),
    resolvers += Resolver.mavenLocal,
    parallelExecution in Test := false,
    scalacOptions := Seq(
      "-unchecked", "-deprecation",
      "-encoding",
      "utf8",
   //   "-Xfatal-warnings",
      "-feature",
      "-language:higherKinds",
   //   "-Xlog-implicits",
      "-language:existentials"
    ),
    javacOptions := Seq("-source", "1.8", "-target", "1.8"),
    libraryDependencies ++= Seq(
      "org.scala-lang"    % "scala-reflect"     % scalaVersion.value,
      "org.scala-lang"    % "scala-compiler"    % scalaVersion.value,
      "org.typelevel"     %% "cats-core"        % CatsVersion, 
      "org.apache.avro"   % "avro"              % AvroVersion,
      "org.slf4j"         % "slf4j-api"         % Slf4jVersion          % "test",
      "log4j"             % "log4j"             % Log4jVersion          % "test",
      "org.slf4j"         % "log4j-over-slf4j"  % Slf4jVersion          % "test",
      "org.scalatest"     %% "scalatest"        % ScalatestVersion      % "test"
    ),
      addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.10")
  )

  val publishingSettings = Seq(
    publishMavenStyle := true,
    publishArtifact in Test := false,
    SbtPgp.autoImport.useGpg := true,
    SbtPgp.autoImport.useGpgAgent := true,
    if (isTravis) {
      credentials += Credentials(
        "Sonatype Nexus Repository Manager",
        "oss.sonatype.org",
        sys.env.getOrElse("OSSRH_USERNAME", ""),
        sys.env.getOrElse("OSSRH_PASSWORD", "")
      )
    } else {
      credentials += Credentials(Path.userHome / ".sbt" / "credentials.sbt")
    },
    if (isTravis) {
      version := s"3.1.0.$travisBuildNumber-SNAPSHOT"
    } else {
      version := "3.0.1"
    },
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isTravis) {
        Some("snapshots" at s"${nexus}content/repositories/snapshots")
      } else {
        Some("releases" at s"${nexus}service/local/staging/deploy/maven2")
      }
    },
    pomExtra := {
      <url>https://github.com/sksamuel/avro4s</url>
        <licenses>
          <license>
            <name>MIT</name>
            <url>https://opensource.org/licenses/MIT</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:sksamuel/avro4s.git</url>
          <connection>scm:git@github.com:sksamuel/avro4s.git</connection>
        </scm>
        <developers>
          <developer>
            <id>sksamuel</id>
            <name>sksamuel</name>
            <url>http://github.com/sksamuel</url>
          </developer>
        </developers>
    }
  )
}

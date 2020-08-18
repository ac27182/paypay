ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.ScalaLibrary
parallelExecution in ThisBuild := false

lazy val coreDependencies =
  Seq(
    "org.apache.spark" %% "spark-sql" % "2.4.5",
    "org.apache.spark" %% "spark-core" % "2.4.5"
  )

lazy val testDependencies =
  Seq(
    "org.scalatest" %% "scalatest" % "3.2.1" % Test,
    "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % Test
  )

lazy val root =
  project
    .in(file("."))
    .aggregate(techTest)

lazy val techTest =
  project
    .in(file("techTest"))
    .settings(
      scalaVersion := "2.12.11",
      assemblyJarName in assembly := "techTest.jar",
      test in assembly := {},
      assemblyMergeStrategy in assembly := {
        case "git.properties"                                             => MergeStrategy.last
        case PathList("javax", "servlet", xs @ _*)                        => MergeStrategy.first
        case PathList("javax", "inject", xs @ _*)                         => MergeStrategy.last
        case PathList("org", "apache", "commons", "collections", xs @ _*) => MergeStrategy.last
        case PathList("org", "apache", "spark", "unused", xs @ _*)        => MergeStrategy.last
        case PathList("org", "aopalliance", "intercept", xs @ _*)         => MergeStrategy.last
        case PathList("org", "aopalliance", "aop", xs @ _*)               => MergeStrategy.last
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      },
      fork in Test := true,
      testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD"),
      resolvers += "spark packages" at "https://dl.bintray.com/spark-packages/maven/",
      libraryDependencies ++= coreDependencies ++ testDependencies
    )

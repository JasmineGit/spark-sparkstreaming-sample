name := "spark-handson"

version := "1.0"


val sparkVersion        = "1.2.0"
val servletVersion      = "3.0.20100224"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "commons-lang" % "commons-lang" % "2.6",
  "org.apache.kafka" %% "kafka" % "0.8.1.1" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
)

    
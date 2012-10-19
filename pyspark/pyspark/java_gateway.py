import glob
import os
from py4j.java_gateway import java_import, JavaGateway


SPARK_HOME = os.environ["SPARK_HOME"]


assembly_jar = glob.glob(os.path.join(SPARK_HOME, "core/target") + \
    "/spark-core-assembly-*.jar")[0]
    # TODO: what if multiple assembly jars are found?


def launch_gateway():
    gateway = JavaGateway.launch_gateway(classpath=assembly_jar,
        javaopts=["-Xmx256m"], die_on_exit=True)
    java_import(gateway.jvm, "spark.api.java.*")
    java_import(gateway.jvm, "spark.api.python.*")
    java_import(gateway.jvm, "scala.Tuple2")
    return gateway

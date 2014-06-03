#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Unit tests for PySpark; additional tests are implemented as doctests in
individual modules.
"""
from fileinput import input
from glob import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile

from pyspark.context import SparkContext
from pyspark.files import SparkFiles
from pyspark.serializers import read_int

_have_scipy = False
try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy, but that's okay, we'll skip those tests
    pass


SPARK_HOME = os.environ["SPARK_HOME"]


class PySparkTestCase(unittest.TestCase):

    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.sc = SparkContext('local[4]', class_name , batchSize=2)

    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path

class TestCheckpoint(PySparkTestCase):

    def setUp(self):
        PySparkTestCase.setUp(self)
        self.checkpointDir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(self.checkpointDir.name)
        self.sc.setCheckpointDir(self.checkpointDir.name)

    def tearDown(self):
        PySparkTestCase.tearDown(self)
        shutil.rmtree(self.checkpointDir.name)

    def test_basic_checkpointing(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: range(1, x + 1))

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        result = flatMappedRDD.collect()
        time.sleep(1)  # 1 second
        self.assertTrue(flatMappedRDD.isCheckpointed())
        self.assertEqual(flatMappedRDD.collect(), result)
        self.assertEqual("file:" + self.checkpointDir.name,
                         os.path.dirname(os.path.dirname(flatMappedRDD.getCheckpointFile())))

    def test_checkpoint_and_restore(self):
        parCollection = self.sc.parallelize([1, 2, 3, 4])
        flatMappedRDD = parCollection.flatMap(lambda x: [x])

        self.assertFalse(flatMappedRDD.isCheckpointed())
        self.assertTrue(flatMappedRDD.getCheckpointFile() is None)

        flatMappedRDD.checkpoint()
        flatMappedRDD.count()  # forces a checkpoint to be computed
        time.sleep(1)  # 1 second

        self.assertTrue(flatMappedRDD.getCheckpointFile() is not None)
        recovered = self.sc._checkpointFile(flatMappedRDD.getCheckpointFile(),
                                            flatMappedRDD._jrdd_deserializer)
        self.assertEquals([1, 2, 3, 4], recovered.collect())


class TestAddFile(PySparkTestCase):

    def test_add_py_file(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this job fails due to `userlibrary` not being on the Python path:
        def func(x):
            from userlibrary import UserClass
            return UserClass().hello()
        self.assertRaises(Exception,
                          self.sc.parallelize(range(2)).map(func).first)
        # Add the file, so the job should now succeed:
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addPyFile(path)
        res = self.sc.parallelize(range(2)).map(func).first()
        self.assertEqual("Hello World!", res)

    def test_add_file_locally(self):
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        self.sc.addFile(path)
        download_path = SparkFiles.get("hello.txt")
        self.assertNotEqual(path, download_path)
        with open(download_path) as test_file:
            self.assertEquals("Hello World!\n", test_file.readline())

    def test_add_py_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlibrary import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlibrary.py")
        self.sc.addFile(path)
        from userlibrary import UserClass
        self.assertEqual("Hello World!", UserClass().hello())

    def test_add_egg_file_locally(self):
        # To ensure that we're actually testing addPyFile's effects, check that
        # this fails due to `userlibrary` not being on the Python path:
        def func():
            from userlib import UserClass
        self.assertRaises(ImportError, func)
        path = os.path.join(SPARK_HOME, "python/test_support/userlib-0.1-py2.7.egg")
        self.sc.addPyFile(path)
        from userlib import UserClass
        self.assertEqual("Hello World from inside a package!", UserClass().hello())


class TestRDDFunctions(PySparkTestCase):

    def test_save_as_textfile_with_unicode(self):
        # Regression test for SPARK-970
        x = u"\u00A1Hola, mundo!"
        data = self.sc.parallelize([x])
        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        data.saveAsTextFile(tempFile.name)
        raw_contents = ''.join(input(glob(tempFile.name + "/part-0000*")))
        self.assertEqual(x, unicode(raw_contents.strip(), "utf-8"))

    def test_transforming_cartesian_result(self):
        # Regression test for SPARK-1034
        rdd1 = self.sc.parallelize([1, 2])
        rdd2 = self.sc.parallelize([3, 4])
        cart = rdd1.cartesian(rdd2)
        result = cart.map(lambda (x, y): x + y).collect()

    def test_cartesian_on_textfile(self):
        # Regression test for
        path = os.path.join(SPARK_HOME, "python/test_support/hello.txt")
        a = self.sc.textFile(path)
        result = a.cartesian(a).collect()
        (x, y) = result[0]
        self.assertEqual("Hello World!", x.strip())
        self.assertEqual("Hello World!", y.strip())

    def test_deleting_input_files(self):
        # Regression test for SPARK-1025
        tempFile = tempfile.NamedTemporaryFile(delete=False)
        tempFile.write("Hello World!")
        tempFile.close()
        data = self.sc.textFile(tempFile.name)
        filtered_data = data.filter(lambda x: True)
        self.assertEqual(1, filtered_data.count())
        os.unlink(tempFile.name)
        self.assertRaises(Exception, lambda: filtered_data.count())


class TestIO(PySparkTestCase):

    def test_stdout_redirection(self):
        import subprocess
        def func(x):
            subprocess.check_call('ls', shell=True)
        self.sc.parallelize([1]).foreach(func)


class TestDaemon(unittest.TestCase):
    def connect(self, port):
        from socket import socket, AF_INET, SOCK_STREAM
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(('127.0.0.1', port))
        # send a split index of -1 to shutdown the worker
        sock.send("\xFF\xFF\xFF\xFF")
        sock.close()
        return True

    def do_termination_test(self, terminator):
        from subprocess import Popen, PIPE
        from errno import ECONNREFUSED

        # start daemon
        daemon_path = os.path.join(os.path.dirname(__file__), "daemon.py")
        daemon = Popen([sys.executable, daemon_path], stdin=PIPE, stdout=PIPE)

        # read the port number
        port = read_int(daemon.stdout)

        # daemon should accept connections
        self.assertTrue(self.connect(port))

        # request shutdown
        terminator(daemon)
        time.sleep(1)

        # daemon should no longer accept connections
        try:
            self.connect(port)
        except EnvironmentError as exception:
            self.assertEqual(exception.errno, ECONNREFUSED)
        else:
            self.fail("Expected EnvironmentError to be raised")

    def test_termination_stdin(self):
        """Ensure that daemon and workers terminate when stdin is closed."""
        self.do_termination_test(lambda daemon: daemon.stdin.close())

    def test_termination_sigterm(self):
        """Ensure that daemon and workers terminate on SIGTERM."""
        from signal import SIGTERM
        self.do_termination_test(lambda daemon: os.kill(daemon.pid, SIGTERM))


class TestSparkSubmit(unittest.TestCase):
    def setUp(self):
        self.programDir = tempfile.mkdtemp()
        self.sparkSubmit = os.path.join(os.environ.get("SPARK_HOME"), "bin", "spark-submit")

    def tearDown(self):
        shutil.rmtree(self.programDir)

    def createTempFile(self, name, content):
        """
        Create a temp file with the given name and content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        path = os.path.join(self.programDir, name)
        with open(path, "w") as f:
            f.write(content)
        return path

    def createFileInZip(self, name, content):
        """
        Create a zip archive containing a file with the given content and return its path.
        Strips leading spaces from content up to the first '|' in each line.
        """
        pattern = re.compile(r'^ *\|', re.MULTILINE)
        content = re.sub(pattern, '', content.strip())
        path = os.path.join(self.programDir, name + ".zip")
        with zipfile.ZipFile(path, 'w') as zip:
            zip.writestr(name, content)
        return path

    def test_single_script(self):
        """Submit and test a single script file"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(lambda x: x * 2).collect()
            """)
        proc = subprocess.Popen([self.sparkSubmit, script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out)

    def test_script_with_local_functions(self):
        """Submit and test a single script file calling a global function"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 3
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(foo).collect()
            """)
        proc = subprocess.Popen([self.sparkSubmit, script], stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[3, 6, 9]", out)

    def test_module_dependency(self):
        """Submit and test a script with a dependency on another module"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(myfunc).collect()
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen([self.sparkSubmit, "--py-files", zip, script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out)

    def test_module_dependency_on_cluster(self):
        """Submit and test a script with a dependency on another module on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |from mylib import myfunc
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(myfunc).collect()
            """)
        zip = self.createFileInZip("mylib.py", """
            |def myfunc(x):
            |    return x + 1
            """)
        proc = subprocess.Popen(
            [self.sparkSubmit, "--py-files", zip, "--master", "local-cluster[1,1,512]", script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 3, 4]", out)

    def test_single_script_on_cluster(self):
        """Submit and test a single script on a cluster"""
        script = self.createTempFile("test.py", """
            |from pyspark import SparkContext
            |
            |def foo(x):
            |    return x * 2
            |
            |sc = SparkContext()
            |print sc.parallelize([1, 2, 3]).map(foo).collect()
            """)
        proc = subprocess.Popen(
            [self.sparkSubmit, "--master", "local-cluster[1,1,512]", script],
            stdout=subprocess.PIPE)
        out, err = proc.communicate()
        self.assertEqual(0, proc.returncode)
        self.assertIn("[2, 4, 6]", out)


@unittest.skipIf(not _have_scipy, "SciPy not installed")
class SciPyTests(PySparkTestCase):
    """General PySpark tests that depend on scipy """

    def test_serialize(self):
        from scipy.special import gammaln
        x = range(1, 5)
        expected = map(gammaln, x)
        observed = self.sc.parallelize(x).map(gammaln).collect()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    if not _have_scipy:
        print "NOTE: Skipping SciPy tests as it does not seem to be installed"
    unittest.main()
    if not _have_scipy:
        print "NOTE: SciPy tests were skipped as it does not seem to be installed"

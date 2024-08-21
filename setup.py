import os
import subprocess
import sys
import setuptools
from setuptools import Extension
from setuptools.command.build_ext import build_ext


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        super().__init__(name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        try:
            subprocess.check_output(["cmake", "--version"])
        except OSError:
            raise RuntimeError(
                "CMake must be installed to build the following extensions: "
                + ", ".join(e.name for e in self.extensions)
            )

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        subprocess.check_call(["cmake", "-S", ext.sourcedir, "-B", "build"])
        subprocess.check_call(["cmake", "--build", "build"])


setuptools.setup(
    name="pydwarfs",
    version="0.10.1",
    author="Marcus Holland-Moritz",
    description="Python bindings for the DwarFS libraries",
    url="https://github.com/mhx/pydwarfs",
    license="GPLv3",
    ext_modules=[CMakeExtension("pydwarfs")],
    cmdclass={"build_ext": CMakeBuild},
    zip_safe=False,
    extras_require={"test": ["pytest>=7.0"]},
)

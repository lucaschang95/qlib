# 用于构建 Qlib 包和编译 Cython 扩展模块
from setuptools import setup, Extension
import numpy
import os


def read(rel_path: str) -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, rel_path), encoding="utf-8") as fp:
        return fp.read()


# 从 __init__.py 文件中提取版本号
def get_version(rel_path: str) -> str:
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")


# 获取 NumPy 的 C 头文件路径，用于编译 Cython 扩展
NUMPY_INCLUDE = numpy.get_include()

VERSION = get_version("qlib/__init__.py")


# 配置包构建，主要是编译两个高性能的 Cython 扩展模块
setup(
    version=VERSION,
    # 定义需要编译的 Cython 扩展模块，用于金融数据的高性能计算
    ext_modules=[
        # 滚动窗口统计计算模块
        Extension(
            "qlib.data._libs.rolling",
            ["qlib/data/_libs/rolling.pyx"],
            language="c++",
            include_dirs=[NUMPY_INCLUDE],
        ),
        # 扩展窗口统计计算模块
        Extension(
            "qlib.data._libs.expanding",
            ["qlib/data/_libs/expanding.pyx"],
            language="c++",
            include_dirs=[NUMPY_INCLUDE],
        ),
    ],
)

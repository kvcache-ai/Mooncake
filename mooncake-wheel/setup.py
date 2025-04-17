from setuptools import setup, Distribution
from wheel.bdist_wheel import bdist_wheel

class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True

class CustomBdistWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False
        self.plat_name_supplied = True
        self.plat_name = "manylinux_2_35_x86_64"

setup(
    distclass=BinaryDistribution,
    cmdclass={
        'bdist_wheel': CustomBdistWheel,
    },
)

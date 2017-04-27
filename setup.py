"""esub python client library"""


from setuptools import setup
import setuphelpers


setup(
    name="esub",
    version=setuphelpers.git_version(),
    description="esub python client library",
    author="Adam Talsma",
    author_email="se-adam@talsma@ccpgames.com",
    url="https://github.com/ccpgames/esub-client/",
    download_url="https://github.com/ccpgames/esub-client/",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Operating System :: POSIX :: Linux",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    py_modules=["esub"],
    long_description=setuphelpers.long_description(),
    cmdclass=setuphelpers.test_command(cover="esub", pdb=True),
    tests_require=["pytest", "pytest-cov", "mock"],
    install_requires=["requests"],
)

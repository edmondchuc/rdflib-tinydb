import pathlib

from setuptools import setup

setup(
    name="rdflib-tinydb",
    version="0.1.0",
    description="TinyDB-backed RDFLib store.",
    long_description=(pathlib.Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    url="https://github.com/edmondchuc/rdflib-tinydb",
    author="Edmond Chuc",
    author_email="edmond.chuc@gmail.com",
    license="BSD-3-Clause",
    platforms=["any"],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: BSD License",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Database Engines/Servers",
    ],
    packages=["rdflib_tinydb"],
    install_requires=["tinydb>=4.5.2,<5.0", "rdflib>=4.0,<7.0"],
    entry_points={
        "rdf.plugins.store": [
            "TinyDBMemory = rdflib_tinydb:TinyDBMemoryStore",
            "TinyDB = rdflib_tinydb:TinyDBStore",
        ]
    },
    include_package_data=True,
)

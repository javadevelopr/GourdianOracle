import setuptools

with open("README.md",'r') as readme:
    long_description = readme.read()


setuptools.setup(
        name="Gourdian-Oracle",
        version="0.1",
        long_description = long_description,
        long_description_content_type = "text/markown",
        packages=setuptools.find_packages(),
        python_requires='>=3.6'
)

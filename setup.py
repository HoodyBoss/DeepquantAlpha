import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="deepquant", # Replace with your own username
    version="0.2.2",
    author="Narong chansoi",
    author_email="narong.minimalist@gmail.com",
    license='MIT',
    description="A core repository of web content for DeepQuant community",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/narongchansoi/DeepQuantAlpha",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
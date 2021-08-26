import setuptools

setuptools.setup(
    name="hcvaEnricher",
    version="0.0.1",
    author="io-commits & Immanuelbh",
    packages=setuptools.find_packages(include=['hcvaEnricher', 'hcvaEnricher.*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)

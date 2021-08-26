import setuptools

setuptools.setup(
    name="hcvaEnricher",
    version="0.0.2",
    author="io-commits & Immanuelbh",
    include_package_data=True,
    packages=setuptools.find_packages('hcvaEnricher'),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)

from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    description="CLI Background Job Queue System with Retries, Backoff & DLQ",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "tabulate>=0.9.0",
    ],
    entry_points={
        "console_scripts": [
            "queuectl = queuectl.cli:cli",
        ],
    },
    python_requires=">=3.8",
)

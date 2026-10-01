import setuptools

setuptools.setup(
    name="crypto-pipeline",
    version="0.1.0",
    description="Binance trades -> Pub/Sub -> Dataflow -> BigQuery",
    packages=setuptools.find_packages(include=["crypto_pipeline", "crypto_pipeline.*"]),
    package_data={"crypto_pipeline": ["schemas/*.json"]},
    include_package_data=True,
    python_requires=">=3.11",
)

from setuptools import find_packages, setup


package_name = "topic_stats"


setup(
    name=package_name,
    version="0.1.0",
    packages=find_packages(exclude=("test",)),
    data_files=[
        (
            "share/ament_index/resource_index/packages",
            [f"resource/{package_name}"],
        ),
        (f"share/{package_name}", ["package.xml", "README.md"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="sava",
    maintainer_email="sava@todo.todo",
    description="Measure ROS 2 topic frequency, time gaps, and burstiness.",
    license="Proprietary",
    entry_points={
        "console_scripts": [
            "topic_stats = topic_stats.topic_stats:main",
        ],
    },
)

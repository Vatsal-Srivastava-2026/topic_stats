from setuptools import find_packages, setup


package_name = "topic_stats"


setup(
    name=package_name,
    version="0.4.2",
    packages=find_packages(exclude=("test",)),
    data_files=[
        (
            "share/ament_index/resource_index/packages",
            [f"resource/{package_name}"],
        ),
        (f"share/{package_name}", ["package.xml", "README.md"]),
        (f"share/{package_name}/config", ["config/live_topic_stats.yaml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="sava",
    maintainer_email="sava@todo.todo",
    description="Measure ROS 2 topic frequency, time gaps, and burstiness.",
    license="Proprietary",
    entry_points={
        "console_scripts": [
            "multi_topic_stats = topic_stats.multi_topic_stats:main",
            "rosbag_stats = topic_stats.rosbag_stats:main",
            "topic_stats = topic_stats.topic_stats:main",
        ],
    },
)

# topic_stats

A standalone ROS 2 Python package for measuring a topic's received frequency,
inter-arrival gaps, and burstiness. It discovers the message type at runtime, so
it works with arbitrary ROS 2 message types available in the sourced environment.

## Build

From the `~/ca1n` workspace:

```bash
source /opt/ros/humble/setup.bash
colcon build --symlink-install --packages-select topic_stats
source install/setup.bash
```

## Run

```bash
ros2 run topic_stats topic_stats /radar/points
```

Useful options:

```bash
# Capture for 60 seconds with custom analysis windows
ros2 run topic_stats topic_stats /radar/points \
  --duration 60 --windows 10 5 1 0.5 0.1 0.05

# Repeated captures, stopping after one completely silent capture
ros2 run topic_stats topic_stats /radar/points --loop

# Force subscriber reliability and choose an output base directory
ros2 run topic_stats topic_stats /radar/points \
  --qos-reliability best_effort --output-dir /tmp/topic_stats
```

Run `ros2 run topic_stats topic_stats --help` for all options.

Unless `--output-dir` is supplied, reports are written under the package's
`out/` directory. Each capture includes CSV data, publisher QoS information,
window summaries, inter-arrival plots, and consolidated burstiness plots.

## Troubleshooting Python package conflicts

If `colcon build` fails inside `setuptools` or `distutils` while loading packages
from `~/.local/lib/python3.10/site-packages`, build with user-site Python
packages disabled:

```bash
source /opt/ros/humble/setup.bash
PYTHONNOUSERSITE=1 colcon build --symlink-install --packages-select topic_stats
```

This setting applies only to the build command and does not uninstall or modify
anything in `~/.local`. Python instead uses the mutually compatible Ubuntu and
ROS Python packages.

Matplotlib must therefore be available as a system package. Install it with:

```bash
sudo apt update
sudo apt install python3-matplotlib
```

You can verify the system installation while ignoring user-site packages with:

```bash
PYTHONNOUSERSITE=1 python3 -c \
  'import matplotlib; print(matplotlib.__version__); print(matplotlib.__file__)'
```

Alternatively, when preparing a ROS workspace, `rosdep` can install declared
dependencies such as `python3-matplotlib`:

```bash
rosdep install --from-paths src --ignore-src -r -y
```

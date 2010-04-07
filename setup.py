from setuptools import setup

setup(
  name="gremlins",
  version="0.1.0",
  packages = ['gremlins'],
  entry_points = {'console_scripts': ['gremlins=gremlins.gremlin:main']},
  )

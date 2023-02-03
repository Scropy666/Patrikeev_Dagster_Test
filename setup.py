from setuptools import setup

setup(
    name='dagster_pipeline',
    version='0.1.0',
    py_modules=['my-proj'],
    install_requires=[
        'Click',
    ],
    # entry_points={
    #     'console_scripts': [
    #         'my-proj = dagster job print -f .\my-proj.py:cli',
    #     ],
    # },
)

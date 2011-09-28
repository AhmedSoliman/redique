from distribute_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

version = '0.1.0'

required_packages =  open("requirements.pip", 'r').readlines()

setup(name='redique',
      description="A super easy Queued RPC on top of Redis",
      long_description="A super easy Queued RPC on top of Redis, with JSON marshalling protocol and exception marshalling",
      version=version,
      url='https://github.com/AhmedSoliman/redique',
      author="Ahmed Soliman",
      author_email="me@ahmedsoliman.com",
      packages=find_packages(),
      zip_safe=False,
      install_requires= required_packages,
      license = "MIT",
      classifiers = ['Development Status :: 4 - Beta',
                     'Intended Audience :: Developers',
                     'Intended Audience :: System Administrators',
                     'License :: OSI Approved :: MIT License',
                     'Operating System :: OS Independent',
                     'Programming Language :: Python :: 2.6',
                     'Programming Language :: Python :: 2.7',
                    ],
      
      )

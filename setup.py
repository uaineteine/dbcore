from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r') as req_file:
        return req_file.read().splitlines()

setup(
    name='dbcore',
    version='1.0',
    author='Daniel Stamer-Squair',
    author_email='uaine.teine@hotmail.com',
    description='This package provides foundational data structures for representing and manipulating tile maps in 2D and 3D environments. Its primary purpose is to enable efficient spatial organization and management of map data for games, simulations, and applications that require robust handling of coordinates, regions, and chunked regions. By offering specialized classes for coordinates, regions, and chunked regions, the package simplifies the development of systems that need precise and scalable map logic.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/uaineteine/dbcore',  
    packages=["dbcore"] ,
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.12',
    install_requires=read_requirements(),
    package_data={'': ['LICENSE']}  # Specify the license file here
)

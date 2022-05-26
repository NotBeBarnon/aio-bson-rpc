import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='aio-bson-rpc',
    version='0.0.1',
    author='NotBeBarNon',
    author_email='1775894560@qq.com',
    description='A Python library for JSON-RPC 2.0',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/NotBeBarnon/aio-bson-rpc',
    packages=setuptools.find_packages(),
    classifiers=(
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ),
)
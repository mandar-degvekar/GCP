import setuptools

REQUIRED_PACKAGES = [
   'apache-beam==2.13.0',
   'tensorflow-transform==0.13.*',
   'tensorflow==1.14.*',
   'pandas==0.25.0',
   'joblib==0.13.2',
   'Keras==2.2.4',
   'numpy==1.16.4',
   'sklearn==0.0',
   'matplotlib==3.1.1'
]

setuptools.setup(
   name='googlestock',
   version='0.0.1',
   install_requires=REQUIRED_PACKAGES,
   packages=setuptools.find_packages(),
   include_package_data=True,
   description='',
)
==============
Foursight-CGAP
==============

A serverless chalice application to monitor and run tasks on `CGAP portal <https://github.com/dbmi-bgm/cgap-portal>`_. Essentially, the app provides an number of endpoints to run checks, fetch results, dynamically create environments to check, and more.


.. image:: https://travis-ci.org/dbmi-bgm/foursight-cgap.svg?branch=production
   :target: https://travis-ci.org/dbmi-bgm/foursight-cgap
   :alt: Build Status

.. image:: https://coveralls.io/repos/github/dbmi-bgm/foursight-cgap/badge.svg?branch=production
   :target: https://coveralls.io/github/dbmi-bgm/foursight-cgap?branch=production
   :alt: Coverage

.. image:: https://readthedocs.org/projects/foursight-cgap/badge/?version=latest
   :target: https://foursight-cgap.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

Beta version
------------

Foursight-CGAP is under active development and features will likely change.


API Documentation
-----------------

Foursight-CGAP uses autodoc to generate documentation for both the core chalicelib and checks. You can find the autodocs in the ``Chalicelib API Documentation`` and ``Check Documentation`` files.


Foursight
---------

For the rest of the documentation, see `Foursight documantation <https://foursight.readthedocs.io/en/latest/>`_.


*Contents*

 .. toctree::
   :maxdepth: 4

   environments
   modules
   check_modules

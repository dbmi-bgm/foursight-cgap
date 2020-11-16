
Foursight-CGAP Environments
===========================

Anatomy of an environment
-------------------------

As an initial disclaimer, Foursight-CGAP environments are specifically made to work with CGAP-portal. Their functionality may be extended in the future, but for now, there is a pretty strict limitation to using Foursight for Fourfront. That said, here are the attributes of an environment:


* ``fourfront``\ : the cgap-portal (fourfront) server address for this environment.
* ``es``\ : the ElasticSearch server address for this environment.
* ``ff_env``\ : the Fourfront environment name, which is generated automatically if not provided.
* ``bucket``\ : the S3 bucket location for check results for this environment, generated automatically.

You can perform a GET request to see the information for any environment (\ ``cgapdev`` in the example below). Please note that you must be logged in as admin to see this information or provide the correct ``Authorization`` header with your request.

.. code-block::

   https://foursight-cgap.hms.harvard.edu/environments/cgapdev


Dynamically creating environments
---------------------------------

Environments can be created with a PUT request to the environments endpoint followed by the new environment name. This will not typically needed to be done, but you can do so with a request containing a JSON body (and the appropriate headers). The JSON must have the ``fourfront`` and ``es`` keys and can optionally have the ``ff_env`` key. If it is not provided, ``ff_env`` will be set to ``fourfront- + <environment name>``.

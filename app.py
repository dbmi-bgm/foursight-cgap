# VERY IMPORTANT:
# In order to support extension via 4dn-cloud-infra,
# this app.py is effectively an alias for chalicelib_cgap/app.py.
# In theory, any repository could include foursight-cgap as a
# dependency and selectively import functionality from
# chalicelib_cgap.app. In the case of 4dn-cloud-infra, we import nothing
# from this file and rewrite the core application code. In the case
# of this repository though, this app.py is the "real" one (and
# notably is not a part of the library). This gives us the best of
# both worlds in that we can use foursight-cgap as an importable
# library while maintaining application (functional) isolation
# and respective differences in environments, check scheduling and
# the check code itself. -- Will Oct 18 2021
from chalicelib_cgap.app import *  # noQA

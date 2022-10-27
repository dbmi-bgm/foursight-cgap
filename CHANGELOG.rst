==============
foursight-cgap
==============


----------
Change Log
----------

2.2.0
=====
* Changes related to Foursight React.
  * Moved all Chalice routes to foursight-core; same with foursight;
    and app-cgap and app-fourfront.py in 4dn-cloud-infra.

2.1.4
=====
* Bring in mamga v1.1.0

2.1.3
=====
* Lifecycle management: Only check files with ``project.lifecycle_management_active=true``. Furthermore, exclude files with status ``uploading`` and ``to be uploaded by workflow`` from the check.

2.1.2
=====
* Assign correct action status when patch_file_lifecycle_status fails.

2.1.1
=====
* Move lifecycle checks to separate group in UI.
* Automatically run action for lifecycle checks.

2.1.0
=====
* Spruced up Foursight UI a bit (virtually all in foursight-core but mentioning here).
  * New header/footer.
    * Different looks for Foursight-CGAP (blue header) and Foursight-Fourfront (green header).
    * More relevant info in header (login email, environment, stage).
  * New /info and /users page.
  * New /users and /users/{email} page.
  * New dropdown to change environments.
  * New logout link.
  * New specific error if login fails due to no user record for environment.

2.0.1
=====
* Work to spruce up the UI a bit.

2.0.0
=====
* Created this CHANGELOG.rst file.
* New version of foursight-core (1.0.0) for work related to C4-826 (IDENTITY-izing Foursight).

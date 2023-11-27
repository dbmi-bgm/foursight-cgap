==============
foursight-cgap
==============


----------
Change Log
----------


4.1.0
=====

* New Portal Reindex page; foursight-core 5.1.0.
* Update poetry to 1.4.2.

4.0.0
=====

* Update to Python 3.11.

3.6.1
=====

* Add ingestion type to POST body for VCF ingestion action

3.6.0
=====

* Changes (to foursight-core) to the access key check; making sure the action does not run every single day.

3.5.0
=====

* Changes in foursight-core (4.3.0) to fix access key check.

3.4.5
=====

* Minor UI fixes for display of status text for checks/actions - in foursight-core.
* Added UI warning for registered action functions with no associated check - in foursight-core.
* Added UI display of Redis info on INFO page - in foursight-core.
* Added a d default .chalice/config.json and removed this from .gitignore

3.4.4
=====

* Update foursight-core 4.1.2.
  Fixes for check arguments not being converted (from string) to int/float/etc as
  appropriate in the React version only (was not calling query_params_to_literals).

3.4.2
=====

* Version changes related to foursight-core changes for SSL certificate and Portal access key checking.
* Using new dcicutils.scripts.publish_to_pypi for publish.

3.3.3
=====

* Dependency updates to update magma + tibanna_ff to fix cost updates on MWFRs
* Lifecycle policy tweaks
  * Remove default deletion of "long term" storage options
  * Remove archived files from search

3.3.2
=====

* Update to foursight-core 3.3.2 (and dcicutils 6.8.0).

3.3.0
=====

* Changes related editing user projects/institutions.
* Removed the trigger_codebuild_run check (in foursight-core now).

3.2.0
=====

* Changes related to support for running actions in Foursight React.

3.1.0
=====

* Updated foursight-core version; changes there related to /accounts page.
  Not actually used currently here, only in foursight.
* Moved lookup of check_setup.json (and accounts.json) to foursight-core,
  from foursight-cgap/chalicelib_cap/app_utils.py.

2.2.0
=====

* Changes related to Foursight React.
  * Renamed chalicelib directory to chalicelib_cgap.
  * Renamed target package (pyproject.toml) from chalicelib to chalicelib_cgap.
  * Moved all Chalice routes to foursight-core (same with foursight).
  * Moved schedules to chalicelib_cgap/check_schedules.py.
  * Using new schedule decorator from foursight_core.schedule_decorator.
  * Added chalicelib_local with a sample check_setup.json suitable for local testing.
  * Changed check_setup.json lookup (in chalicelib_cgap/app_utils.py) to look for the
    above local file if CHALICE_LOCAL environment variable set to "1"; and also to look
    for check_setup.json in the directory specified by the FOURSIGHT_CHECK_SETUP_DIR environment
    variable, if set, otherwise look in the local chalicelib_cgap directory; and setup a fallback
    directory for this lookup to this local chalicelib_cgap directory, which foursight-core will
    use if there is no (non-empty) check_setup.json in the specified directory.

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

==============
foursight-cgap
==============


----------
Change Log
----------

2.2.1
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

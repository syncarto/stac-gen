sat-stac source has been included directly in the stac-gen repo due to limitations with being unable to add a github fork as a pip dependency. So the choice was between including the sat-stac source here, or publishing the fork to PyPI. I chose to include the source directly since that will make further edits to sat-stac much easier than maintaining a separate PyPI package.

This source is copied from my sat-stac fork on the following branch:
https://github.com/mhiley/sat-stac/tree/fix-s3-self-urls

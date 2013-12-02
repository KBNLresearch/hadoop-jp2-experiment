Simple Hadoop conversion workflow using external tools
========================

Use Hadoop map jobs to spawn single tiff to jp2 conversion- and validationworkflow by spawning external binaries on the local OS.

Sadly some external binaries spawned in this setup are proprietary software:
- kdu_expand
- awaredriver (wrapped by the jp2wrappa.py)

They could however be interchanged with other binaries as long as those are available through the local filesystem's PATH variable.
Most of the logic is contained in the ConversionMapper source.

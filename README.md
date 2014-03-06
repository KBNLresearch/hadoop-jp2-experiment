Simple Hadoop conversion workflow using external tools
========================

Use [Hadoop](http://hadoop.apache.org/) map jobs to spawn single TIFF to JP2 conversion- and validation workflow using external binaries on the local OS.
This setup aims to emulate the actual workflow used for the TIFF to JP2 migration programme of the [KB](http://kb.nl/en/research) (National Library of the Netherlands).


Sadly some external binaries spawned in this setup are proprietary software:
- [kdu_expand](http://www.kakadusoftware.com/)
- [awaredriver](http://www.aware.com/imaging/jpeg2000sdk.html) (wrapped by the [jp2wrappa.py](https://github.com/openplanets/jpwrappa))

They could however be interchanged with other binaries as long as those are available through the local filesystem's PATH variable.
Most of the logic is contained in the ConversionMapper source.
